import logging
import pathlib
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from aiohttp import web
from aiohttp.web_request import Request

from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupID, WorkerGroupNotFoundError
from scaler.worker_adapter.orb_adapter.config import ORBWorkerAdapterConfig

# Import from open_resource_broker - works with both:
# 1. Submodule installed via `uv pip install .` from src/scaler/worker_adapter/open_resource_broker/
# 2. PyPI package after `pip install open-resource-broker`
from open_resource_broker import api as orb_api

logger = logging.getLogger(__name__)


@dataclass
class WorkerGroupInfo:
    """Information about a worker group managed by ORB."""

    request_id: str
    template_id: str
    worker_count: int
    worker_names: List[str] = field(default_factory=list)


class ORBWorkerAdapter:
    """Worker adapter that uses Open Resource Broker (ORB) for pod provisioning.

    This adapter translates Scaler's worker lifecycle requests into ORB API calls,
    enabling Scaler to leverage ORB's cloud-agnostic Kubernetes pod management.

    The ORB watcher processes must be running separately:
    - watch_pods: monitors Kubernetes pod events
    - watch_requests: processes machine creation requests
    - watch_return_requests: processes machine termination requests
    """

    def __init__(self, config: ORBWorkerAdapterConfig):
        self._workdir = pathlib.Path(config.orb_workdir)
        self._templates = pathlib.Path(config.orb_templates)
        self._template_id = config.orb_template_id
        self._max_workers = config.worker_adapter_config.max_workers
        self._workers_per_group = config.workers_per_group
        self._adapter_web_host = config.web_config.adapter_web_host
        self._adapter_web_port = config.web_config.adapter_web_port

        # Track worker groups: group_id -> WorkerGroupInfo
        self._worker_groups: Dict[WorkerGroupID, WorkerGroupInfo] = {}

        # Validate ORB templates on startup
        self._validate_template()

    def _validate_template(self) -> None:
        """Validate that the configured template exists in ORB templates file."""
        try:
            templates = orb_api.get_available_templates(str(self._templates))
            template_ids = [t["templateId"] for t in templates.get("templates", [])]
            if self._template_id not in template_ids:
                raise ValueError(
                    f"Template ID '{self._template_id}' not found in templates file. "
                    f"Available templates: {template_ids}"
                )
            logger.info("ORB adapter initialized with template '%s' from %s", self._template_id, self._templates)
        except Exception as e:
            logger.error("Failed to validate ORB template: %s", e)
            raise

    def _count_total_workers(self) -> int:
        """Count total workers across all groups."""
        return sum(info.worker_count for info in self._worker_groups.values())

    async def start_worker_group(self) -> WorkerGroupID:
        """Start a new worker group by requesting machines from ORB.

        Returns:
            The unique worker group ID.

        Raises:
            CapacityExceededError: If max_workers limit would be exceeded.
        """
        current_workers = self._count_total_workers()
        if self._max_workers != -1 and current_workers + self._workers_per_group > self._max_workers:
            raise CapacityExceededError(
                f"Maximum number of workers ({self._max_workers}) would be exceeded. "
                f"Current: {current_workers}, requested: {self._workers_per_group}"
            )

        # Generate unique request ID for ORB
        request_id = f"scaler-{uuid.uuid4().hex[:12]}"
        worker_group_id = f"orb-{request_id}".encode()

        logger.info(
            "Starting worker group %s with %d workers using template '%s'",
            request_id,
            self._workers_per_group,
            self._template_id,
        )

        # Request machines from ORB
        result = orb_api.request_machines(
            workdir=str(self._workdir),
            templates=str(self._templates),
            template_id=self._template_id,
            count=self._workers_per_group,
            request_id=request_id,
        )

        logger.info("ORB request_machines result: %s", result)

        # Generate expected worker names (ORB naming convention)
        worker_names = [f"{request_id}-{i}" for i in range(self._workers_per_group)]

        # Track the worker group
        self._worker_groups[worker_group_id] = WorkerGroupInfo(
            request_id=request_id,
            template_id=self._template_id,
            worker_count=self._workers_per_group,
            worker_names=worker_names,
        )

        return worker_group_id

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> None:
        """Shutdown a worker group by requesting machine return from ORB.

        Args:
            worker_group_id: The ID of the worker group to shutdown.

        Raises:
            WorkerGroupNotFoundError: If the worker group doesn't exist.
        """
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist.")

        group_info = self._worker_groups[worker_group_id]
        return_request_id = f"return-{group_info.request_id}"

        logger.info("Shutting down worker group %s (request_id=%s)", worker_group_id.decode(), group_info.request_id)

        # Build machine list for ORB return request
        machines = [{"name": name} for name in group_info.worker_names]

        # Request machine return from ORB
        result = orb_api.request_return_machines(
            workdir=str(self._workdir), machines=machines, request_id=return_request_id
        )

        logger.info("ORB request_return_machines result: %s", result)

        # Remove from tracking
        self._worker_groups.pop(worker_group_id)

    async def get_worker_group_status(self, worker_group_id: WorkerGroupID) -> Optional[dict]:
        """Get the status of a worker group from ORB.

        Args:
            worker_group_id: The ID of the worker group.

        Returns:
            Status dict from ORB, or None if group not found.
        """
        if worker_group_id not in self._worker_groups:
            return None

        group_info = self._worker_groups[worker_group_id]

        status = orb_api.get_request_status(workdir=str(self._workdir), hf_req_ids=[group_info.request_id])

        return status

    async def webhook_handler(self, request: Request) -> web.Response:
        """Handle webhook requests from the Scaler scheduler.

        Supported actions:
        - get_worker_adapter_info: Return adapter capabilities
        - start_worker_group: Create a new worker group
        - shutdown_worker_group: Terminate a worker group
        - get_worker_group_status: Get status of a worker group (ORB-specific)
        """
        request_json = await request.json()

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=web.HTTPBadRequest.status_code)

        action = request_json["action"]

        if action == "get_worker_adapter_info":
            return web.json_response(
                {
                    "max_worker_groups": (
                        self._max_workers // self._workers_per_group if self._max_workers != -1 else -1
                    ),
                    "workers_per_group": self._workers_per_group,
                    "base_capabilities": {},  # ORB pods define their own capabilities
                    "adapter_type": "orb",
                    "template_id": self._template_id,
                },
                status=web.HTTPOk.status_code,
            )

        elif action == "start_worker_group":
            try:
                worker_group_id = await self.start_worker_group()
            except CapacityExceededError as e:
                return web.json_response({"error": str(e)}, status=web.HTTPTooManyRequests.status_code)
            except Exception as e:
                logger.exception("Failed to start worker group")
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            group_info = self._worker_groups[worker_group_id]
            return web.json_response(
                {
                    "status": "Worker group started",
                    "worker_group_id": worker_group_id.decode(),
                    "worker_ids": group_info.worker_names,
                    "request_id": group_info.request_id,
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
                logger.exception("Failed to shutdown worker group")
                return web.json_response({"error": str(e)}, status=web.HTTPInternalServerError.status_code)

            return web.json_response({"status": "Worker group shutdown"}, status=web.HTTPOk.status_code)

        elif action == "get_worker_group_status":
            if "worker_group_id" not in request_json:
                return web.json_response(
                    {"error": "No worker_group_id specified"}, status=web.HTTPBadRequest.status_code
                )

            worker_group_id = request_json["worker_group_id"].encode()
            status = await self.get_worker_group_status(worker_group_id)

            if status is None:
                return web.json_response(
                    {"error": f"Worker group {worker_group_id.decode()} not found"}, status=web.HTTPNotFound.status_code
                )

            return web.json_response(status, status=web.HTTPOk.status_code)

        else:
            return web.json_response({"error": f"Unknown action: {action}"}, status=web.HTTPBadRequest.status_code)

    def create_app(self) -> web.Application:
        """Create the aiohttp web application."""
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app
