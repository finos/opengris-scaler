import os
import signal
import uuid
from typing import Dict, Optional, Tuple

from aiohttp import web
from aiohttp.web_request import Request

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.identifiers import WorkerID
from scaler.worker.worker import Worker
from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupID, WorkerGroupNotFoundError


class NativeWorkerAdapter:
    def __init__(
        self,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageConfig],
        capabilities: Dict[str, int],
        io_threads: int,
        task_queue_size: int,
        max_workers: int,
        heartbeat_interval_seconds: int,
        task_timeout_seconds: int,
        death_timeout_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_processor_suspend: bool,
        event_loop: str,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
    ):
        self._address = address
        self._object_storage_address = object_storage_address
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._task_queue_size = task_queue_size
        self._max_workers = max_workers
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._task_timeout_seconds = task_timeout_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._hard_processor_suspend = hard_processor_suspend
        self._event_loop = event_loop
        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        """
        Although a worker group can contain multiple workers, in this native adapter implementation,
        each worker group will only contain one worker.
        """
        self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, Worker]] = {}

    async def start_worker_group(self) -> WorkerGroupID:
        num_of_workers = sum(len(workers) for workers in self._worker_groups.values())
        if num_of_workers >= self._max_workers != -1:
            raise CapacityExceededError(f"Maximum number of workers ({self._max_workers}) reached.")

        worker = Worker(
            name=f"NAT|{uuid.uuid4().hex}",
            address=self._address,
            object_storage_address=self._object_storage_address,
            preload=None,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            task_timeout_seconds=self._task_timeout_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            event_loop=self._event_loop,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
        )

        worker.start()
        worker_group_id = f"native-{uuid.uuid4().hex}".encode()
        self._worker_groups[worker_group_id] = {worker.identity: worker}
        return worker_group_id

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist.")

        for worker in self._worker_groups[worker_group_id].values():
            os.kill(worker.pid, signal.SIGINT)
            worker.join()

        self._worker_groups.pop(worker_group_id)

    async def webhook_handler(self, request: Request):
        request_json = await request.json()

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=web.HTTPBadRequest.status_code)

        action = request_json["action"]

        if action == "get_worker_adapter_info":
            return web.json_response(
                {
                    "max_worker_groups": self._max_workers,
                    "workers_per_group": 1,
                    "base_capabilities": self._capabilities,
                },
                status=web.HTTPOk.status_code,
            )

        elif action == "start_worker_group":
            try:
                worker_group_id = await self.start_worker_group()
            except CapacityExceededError as e:
                return web.json_response({"error": str(e)}, status=web.HTTPTooManyRequests.status_code)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response(
                {
                    "status": "Worker group started",
                    "worker_group_id": worker_group_id.decode(),
                    "worker_ids": [worker_id.decode() for worker_id in self._worker_groups[worker_group_id].keys()],
                },
                status=web.HTTPOk.status_code,
            )

        elif action == "shutdown_worker_group":
            if "worker_group_id" not in request_json:
                return web.json_response(
                    {"error": "No worker_group_id specified"}, status=web.HTTPBadRequest.status_code
                )

            worker_group_id = request_json["worker_group_id"].encode()
            try:
                await self.shutdown_worker_group(worker_group_id)
            except WorkerGroupNotFoundError as e:
                return web.json_response({"error": str(e)}, status=web.HTTPNotFound.status_code)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response({"status": "Worker group shutdown"}, status=web.HTTPOk.status_code)

        else:
            return web.json_response({"error": "Unknown action"}, status=web.HTTPBadRequest.status_code)

    def create_app(self):
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app
