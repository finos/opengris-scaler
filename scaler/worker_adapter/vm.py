import os
import signal
from typing import Dict, Optional, Tuple

from aiohttp import web
from aiohttp.web_request import Request

from scaler.utility.identifiers import WorkerID
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.worker import Worker


class VMWorkerAdapter:
    def __init__(
        self,
        address: ZMQConfig,
        storage_address: Optional[ObjectStorageConfig],
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
        self._storage_address = storage_address
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

        self._workers: Dict[WorkerID, Worker] = {}

    async def start_worker(self, worker_id: str) -> Worker:
        if len(self._workers) >= self._max_workers != -1:
            raise RuntimeError(f"Maximum number of workers ({self._max_workers}) reached.")

        worker = Worker(
            name=worker_id,
            address=self._address,
            storage_address=self._storage_address,
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
        self._workers[worker.identity] = worker
        return worker

    async def shutdown_worker(self, worker_id: str) -> Worker:
        worker_id = WorkerID(worker_id.encode())

        if worker_id not in self._workers:
            raise ValueError(f"Worker with ID {worker_id} does not exist.")

        worker = self._workers[worker_id]
        os.kill(worker.pid, signal.SIGINT)
        worker.join()
        self._workers.pop(worker_id)
        return worker

    async def webhook_handler(self, request: Request):
        request_json = await request.json()

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=400)

        if "worker_id" not in request_json:
            return web.json_response({"error": "No worker_id specified"}, status=400)

        action = request_json["action"]
        worker_id = request_json["worker_id"]

        if action == "start_worker":
            try:
                worker = await self.start_worker(worker_id)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=500)

            return web.json_response({"status": "Worker started", "worker_id": worker.identity.decode()}, status=200)

        elif action == "shutdown_worker":
            try:
                worker = await self.shutdown_worker(worker_id)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=404)
            except Exception as e:
                return web.json_response({"error": str(e)}, status=500)

            return web.json_response({"status": "Worker shutdown", "worker_id": worker.identity.decode()}, status=200)

        else:
            return web.json_response({"error": "Unknown action"}, status=400)

    def create_app(self):
        """Create and configure the aiohttp application"""
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app
