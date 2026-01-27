import logging
import math
from typing import Dict, List

import aiohttp
from aiohttp import web

from scaler.protocol.python.message import InformationSnapshot
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerGroupID
from scaler.utility.identifiers import WorkerID


class VanillaScalingController(ScalingController):
    def __init__(self, adapter_webhook_url: str):
        self._adapter_webhook_url = adapter_webhook_url
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

        self._worker_groups: Dict[WorkerGroupID, List[WorkerID]] = {}

    def get_status(self):
        return ScalingManagerStatus.new_msg(worker_groups=self._worker_groups)

    async def on_snapshot(self, information_snapshot: InformationSnapshot):
        if not information_snapshot.workers:
            if information_snapshot.tasks:
                await self._start_worker_group()
            return

        task_ratio = len(information_snapshot.tasks) / len(information_snapshot.workers)
        if task_ratio > self._upper_task_ratio:
            await self._start_worker_group()
        elif task_ratio < self._lower_task_ratio:
            worker_group_task_counts = {
                worker_group_id: sum(
                    information_snapshot.workers[worker_id].queued_tasks
                    for worker_id in worker_ids
                    if worker_id in information_snapshot.workers
                )
                for worker_group_id, worker_ids in self._worker_groups.items()
            }
            if not worker_group_task_counts:
                logging.warning(
                    "No worker groups available to shut down. There might be statically provisioned workers."
                )
                return

            worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)
            await self._shutdown_worker_group(worker_group_id)

    async def _start_worker_group(self):
        response, status = await self._make_request({"action": "get_worker_adapter_info"})
        if status != web.HTTPOk.status_code:
            logging.warning("Failed to get worker adapter info.")
            return

        if len(self._worker_groups) >= response.get("max_worker_groups", math.inf):
            return

        response, status = await self._make_request({"action": "start_worker_group"})
        if status == web.HTTPTooManyRequests.status_code:
            logging.warning("Capacity exceeded, cannot start new worker group.")
            return
        if status == web.HTTPInternalServerError.status_code:
            logging.error(f"Failed to start worker group: {response.get('error', 'Unknown error')}")
            return

        worker_group_id = response["worker_group_id"].encode()
        self._worker_groups[worker_group_id] = [WorkerID(worker_id.encode()) for worker_id in response["worker_ids"]]
        logging.info(f"Started worker group: {worker_group_id.decode()}")

    async def _shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        if worker_group_id not in self._worker_groups:
            logging.error(f"Worker group with ID {worker_group_id.decode()} does not exist.")
            return

        response, status = await self._make_request(
            {"action": "shutdown_worker_group", "worker_group_id": worker_group_id.decode()}
        )
        if status == web.HTTPNotFound.status_code:
            logging.error(f"Worker group with ID {worker_group_id.decode()} not found in adapter.")
            return
        if status == web.HTTPInternalServerError.status_code:
            logging.error(f"Failed to shutdown worker group: {response.get('error', 'Unknown error')}")
            return

        self._worker_groups.pop(worker_group_id)
        logging.info(f"Shutdown worker group: {worker_group_id.decode()}")

    async def _make_request(self, payload):
        async with aiohttp.ClientSession() as session:
            async with session.post(self._adapter_webhook_url, json=payload) as response:
                return await response.json(), response.status
