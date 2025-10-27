import os
import signal
import uuid
from typing import Dict

from aiohttp import web
from aiohttp.web_request import Request

from scaler.config.section.symphony_worker_adapter import SymphonyWorkerConfig
from scaler.utility.identifiers import WorkerID
from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupID, WorkerGroupNotFoundError
from scaler.worker_adapter.symphony.worker import SymphonyWorker


class SymphonyWorkerAdapter:
    def __init__(self, config: SymphonyWorkerConfig):
        self._symphony_worker_adapter_config = config

        """
        Although a worker group can contain multiple workers, in this Symphony adapter implementation,
        there will be only one worker group which contains one Symphony worker.
        """
        self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, SymphonyWorker]] = {}

    async def start_worker_group(self) -> WorkerGroupID:
        if self._worker_groups:
            raise CapacityExceededError("Symphony worker already started")

        worker = SymphonyWorker(
            name=f"SYM|{uuid.uuid4().hex}",
            address=self._symphony_worker_adapter_config.scheduler_address,
            object_storage_address=self._symphony_worker_adapter_config.object_storage_address,
            service_name=self._symphony_worker_adapter_config.service_name,
            base_concurrency=self._symphony_worker_adapter_config.base_concurrency,
            capabilities=self._symphony_worker_adapter_config.worker_capabilities.capabilities,
            io_threads=self._symphony_worker_adapter_config.io_threads,
            task_queue_size=self._symphony_worker_adapter_config.worker_task_queue_size,
            heartbeat_interval_seconds=self._symphony_worker_adapter_config.heartbeat_interval,
            death_timeout_seconds=self._symphony_worker_adapter_config.death_timeout_seconds,
            event_loop=self._symphony_worker_adapter_config.event_loop,
        )

        worker.start()
        worker_group_id = f"symphony-{uuid.uuid4().hex}".encode()
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
                    "max_worker_groups": 1,
                    "workers_per_group": 1,
                    "base_capabilities": self._symphony_worker_adapter_config.worker_capabilities.capabilities,
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
