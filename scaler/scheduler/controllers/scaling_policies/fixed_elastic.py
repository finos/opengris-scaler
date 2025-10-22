import logging
import math
from typing import Dict, List, Literal, Optional

import aiohttp
from aiohttp import web

from scaler.protocol.python.message import InformationSnapshot
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.scaling_policies.mixins import ScalingController
from scaler.scheduler.controllers.scaling_policies.types import WorkerGroupID
from scaler.utility.identifiers import WorkerID

WorkerAdapterLabel = Literal["primary", "secondary"]


class FixedElasticScalingController(ScalingController):
    def __init__(self, primary_adapter_webhook_url: str, secondary_adapter_webhook_url: str):
        self._primary_webhook = primary_adapter_webhook_url
        self._secondary_webhook = secondary_adapter_webhook_url
        self._primary_group_limit = 1
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

        self._worker_groups: Dict[WorkerGroupID, List[WorkerID]] = {}
        self._worker_group_source: Dict[WorkerGroupID, WorkerAdapterLabel] = {}

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
                logging.warning("No worker groups available to shut down.")
                return

            # Prefer shutting down secondary adapter groups first
            secondary_groups = [
                (group_id, task_count)
                for group_id, task_count in worker_group_task_counts.items()
                if self._worker_group_source.get(group_id) == "secondary"
            ]
            if secondary_groups:
                worker_group_id = min(secondary_groups, key=lambda item: item[1])[0]
            else:
                worker_group_id = min(worker_group_task_counts, key=worker_group_task_counts.get)

            await self._shutdown_worker_group(worker_group_id)

    async def _start_worker_group(self):
        # Select adapter: use primary if under limit, otherwise use secondary
        adapter: Optional[WorkerAdapterLabel] = None
        webhook = None

        if self._primary_webhook:
            primary_count = sum(source == "primary" for source in self._worker_group_source.values())
            if self._primary_group_limit is None or primary_count < self._primary_group_limit:
                adapter = "primary"
                webhook = self._primary_webhook
            else:
                logging.debug(f"Primary adapter worker group limit reached ({self._primary_group_limit}).")

        if adapter is None and self._secondary_webhook:
            adapter = "secondary"
            webhook = self._secondary_webhook

        if adapter is None:
            logging.warning("All worker adapters have reached their capacity; cannot start a new worker group.")
            return

        response, status = await self._make_request(webhook, {"action": "get_worker_adapter_info"})
        if status != web.HTTPOk.status_code:
            logging.warning("Failed to get worker adapter info.")
            return

        if sum(adapter == "secondary" for adapter in self._worker_group_source.values()) >= response.get(
            "max_worker_groups", math.inf
        ):
            return

        response, status = await self._make_request(webhook, {"action": "start_worker_group"})
        if status == web.HTTPTooManyRequests.status_code:
            logging.warning(f"{adapter.capitalize()} adapter capacity exceeded, cannot start new worker group.")
            return
        if status == web.HTTPInternalServerError.status_code:
            logging.error(
                f"{adapter.capitalize()} adapter failed to start worker group:"
                f" {response.get('error', 'Unknown error')}"
            )
            return

        worker_group_id = response["worker_group_id"].encode()
        self._worker_groups[worker_group_id] = [WorkerID(worker_id.encode()) for worker_id in response["worker_ids"]]
        self._worker_group_source[worker_group_id] = adapter
        logging.info(f"Started worker group {worker_group_id.decode()} on {adapter} adapter.")

    async def _shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        if worker_group_id not in self._worker_groups:
            logging.error(f"Worker group with ID {worker_group_id.decode()} does not exist.")
            return

        adapter = self._worker_group_source.get(worker_group_id)
        if adapter is None:
            logging.error(f"Worker group {worker_group_id.decode()} has no associated adapter recorded.")
            return

        webhook = self._primary_webhook if adapter == "primary" else self._secondary_webhook
        response, status = await self._make_request(
            webhook, {"action": "shutdown_worker_group", "worker_group_id": worker_group_id.decode()}
        )
        if status == web.HTTPNotFound.status_code:
            logging.error(f"Worker group with ID {worker_group_id.decode()} not found in {adapter} adapter.")
            return
        if status == web.HTTPInternalServerError.status_code:
            logging.error(
                f"{adapter.capitalize()} adapter failed to shutdown worker group:"
                f" {response.get('error', 'Unknown error')}"
            )
            return

        self._worker_groups.pop(worker_group_id)
        self._worker_group_source.pop(worker_group_id)
        logging.info(f"Shutdown worker group {worker_group_id.decode()} on {adapter} adapter.")

    @staticmethod
    async def _make_request(webhook_url: str, payload):
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload) as response:
                return await response.json(), response.status
