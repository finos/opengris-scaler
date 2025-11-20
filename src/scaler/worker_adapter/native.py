import os
import signal
import uuid
from typing import Dict

from aiohttp import web
from aiohttp.web_request import Request

from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.utility.identifiers import WorkerID
from scaler.worker.worker import Worker
from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupID, WorkerGroupNotFoundError


class NativeWorkerAdapter:
    def __init__(self, config: NativeWorkerAdapterConfig):
        self._address = config.worker_adapter_config.scheduler_address
        self._object_storage_address = config.worker_adapter_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_workers = config.worker_adapter_config.max_workers
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.event_loop
        self._adapter_web_host = config.web_config.adapter_web_host
        self._adapter_web_port = config.web_config.adapter_web_port
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

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
