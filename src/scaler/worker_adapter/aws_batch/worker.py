"""
AWS Batch Worker.

Connects to the Scaler scheduler via ZMQ streaming and forwards tasks
to AWS Batch for execution. This is the main process that bridges
the scheduler stream to AWS Batch.
"""

import asyncio
import logging
import multiprocessing
import signal
from collections import deque
from typing import Dict, Optional

import zmq.asyncio

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.io.utility import create_async_object_storage_connector
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    ObjectInstruction,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker_adapter.aws_batch.heartbeat_manager import AWSBatchHeartbeatManager
from scaler.worker_adapter.aws_batch.worker_adapter import AWSBatchWorkerAdapter


class AWSBatchWorker(multiprocessing.get_context("spawn").Process):
    """
    AWS Batch Worker that receives tasks from scheduler stream
    and submits them to AWS Batch.
    """

    def __init__(
        self,
        name: str,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageConfig],
        job_queue: str,
        job_definition: str,
        aws_region: str,
        s3_bucket: str,
        s3_prefix: str = "scaler-tasks",
        capabilities: Optional[Dict[str, int]] = None,
        max_concurrent_jobs: int = 100,
        heartbeat_interval_seconds: int = 1,
        death_timeout_seconds: int = 30,
        poll_interval_seconds: float = 1.0,
        io_threads: int = 2,
        event_loop: str = "builtin",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        multiprocessing.Process.__init__(self, name="AWSBatchWorker")
        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._object_storage_address = object_storage_address
        self._capabilities = capabilities or {}
        self._io_threads = io_threads
        self._ident = WorkerID.generate_worker_id(name)

        self._job_queue = job_queue
        self._job_definition = job_definition
        self._aws_region = aws_region
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._max_concurrent_jobs = max_concurrent_jobs
        self._poll_interval = poll_interval_seconds
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds

        self._context: Optional[zmq.asyncio.Context] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._adapter: Optional[AWSBatchWorkerAdapter] = None
        self._heartbeat_manager: Optional[AWSBatchHeartbeatManager] = None
        self._timeout_manager: Optional[VanillaTimeoutManager] = None

        self._heartbeat_received: bool = False
        self._backoff_message_queue: deque = deque()

    @property
    def identity(self) -> WorkerID:
        return self._ident

    def run(self) -> None:
        self._initialize()
        self._run_forever()

    def _initialize(self):
        setup_logger()
        register_event_loop(self._event_loop)

        self._context = zmq.asyncio.Context(io_threads=self._io_threads)
        self._connector_external = ZMQAsyncConnector(
            context=self._context,
            name=self._name,
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self._on_receive_external,
            identity=self._ident,
        )

        self._connector_storage = create_async_object_storage_connector()

        # Create AWS Batch adapter
        self._adapter = AWSBatchWorkerAdapter(
            job_queue=self._job_queue,
            job_definition=self._job_definition,
            aws_region=self._aws_region,
            s3_bucket=self._s3_bucket,
            s3_prefix=self._s3_prefix,
            max_concurrent_jobs=self._max_concurrent_jobs,
            poll_interval_seconds=self._poll_interval,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )

        self._heartbeat_manager = AWSBatchHeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._max_concurrent_jobs,
        )

        self._timeout_manager = VanillaTimeoutManager(
            death_timeout_seconds=self._death_timeout_seconds
        )

        # Register components
        self._adapter.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
        )
        self._heartbeat_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            worker_task_manager=self._adapter,
            timeout_manager=self._timeout_manager,
        )

        self._loop = asyncio.get_event_loop()
        self._register_signal()
        self._task = self._loop.create_task(self._get_loops())

    async def _on_receive_external(self, message: Message):
        if isinstance(message, WorkerHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            self._heartbeat_received = True
            # Process backoff queue
            while self._backoff_message_queue:
                backoff_msg = self._backoff_message_queue.popleft()
                await self._process_message(backoff_msg)
            return

        if not self._heartbeat_received:
            self._backoff_message_queue.append(message)
            return

        await self._process_message(message)

    async def _process_message(self, message: Message):
        if isinstance(message, Task):
            await self._adapter.on_task(message)
            return

        if isinstance(message, TaskCancel):
            await self._adapter.on_task_cancel(message)
            return

        if isinstance(message, ObjectInstruction):
            # Handle object instructions if needed
            logging.debug(f"Received object instruction: {message}")
            return

        if isinstance(message, ClientDisconnect):
            if message.disconnect_type == ClientDisconnect.DisconnectType.Shutdown:
                raise ClientShutdownException("received client shutdown")
            logging.error(f"Worker received invalid ClientDisconnect: {message=}")
            return

        logging.warning(f"Unknown message type: {type(message)}")

    async def _get_loops(self):
        if self._object_storage_address is not None:
            await self._connector_storage.connect(
                self._object_storage_address.host,
                self._object_storage_address.port
            )

        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._adapter.routine, self._poll_interval),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"{self.identity!r}: {str(e)}")
        except Exception as e:
            logging.exception(f"{self.identity!r}: failed with exception: {e}")

        await self._connector_external.send(DisconnectRequest.new_msg(self.identity))
        self._connector_external.destroy()
        logging.info(f"{self.identity!r}: quit")

    def _run_forever(self):
        self._loop.run_until_complete(self._task)

    def _register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self._destroy)

    def _destroy(self):
        self._task.cancel()
