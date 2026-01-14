"""Dummy ORB Worker Adapter for testing without Kubernetes or the actual ORB package.

This module provides a mock implementation that simulates ORB behavior,
allowing you to test the Scaler-ORB integration locally without:
- A running Kubernetes cluster
- The actual open-resource-broker package installed
- ORB watcher processes running

Usage:
    from scaler.worker_adapter.orb_adapter.dummy_adapter import DummyORBWorkerAdapter
    adapter = DummyORBWorkerAdapter(config)
"""

import logging
import uuid
from dataclasses import dataclass, field
from typing import Dict, List

from aiohttp import web
from aiohttp.web_request import Request

from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupID, WorkerGroupNotFoundError

logger = logging.getLogger(__name__)


@dataclass
class DummyWorkerGroupInfo:
    """Information about a simulated worker group."""

    request_id: str
    template_id: str
    worker_count: int
    worker_names: List[str] = field(default_factory=list)
    status: str = "running"


@dataclass
class DummyORBConfig:
    """Simple config for the dummy adapter."""

    adapter_web_host: str = "127.0.0.1"
    adapter_web_port: int = 8080
    max_workers: int = 10
    workers_per_group: int = 1
    template_id: str = "dummy-template"


class DummyORBWorkerAdapter:
    """Dummy worker adapter that mocks ORB behavior for testing.

    This adapter simulates the ORB request/response cycle without actually
    creating any pods. Useful for:
    - Testing the Scaler scheduler's scaling controller
    - Integration testing without Kubernetes
    - Development and debugging of the adapter interface
    """

    def __init__(self, config: DummyORBConfig = None):
        config = config or DummyORBConfig()
        self._adapter_web_host = config.adapter_web_host
        self._adapter_web_port = config.adapter_web_port
        self._max_workers = config.max_workers
        self._workers_per_group = config.workers_per_group
        self._template_id = config.template_id

        self._worker_groups: Dict[WorkerGroupID, DummyWorkerGroupInfo] = {}

        logger.info(
            "DummyORBWorkerAdapter initialized (max_workers=%d, workers_per_group=%d)",
            self._max_workers,
            self._workers_per_group,
        )

    def _count_total_workers(self) -> int:
        """Count total workers across all groups."""
        return sum(info.worker_count for info in self._worker_groups.values())

    async def start_worker_group(self) -> WorkerGroupID:
        """Simulate starting a worker group.

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

        request_id = f"dummy-{uuid.uuid4().hex[:12]}"
        worker_group_id = f"orb-{request_id}".encode()

        worker_names = [f"{request_id}-{i}" for i in range(self._workers_per_group)]

        self._worker_groups[worker_group_id] = DummyWorkerGroupInfo(
            request_id=request_id,
            template_id=self._template_id,
            worker_count=self._workers_per_group,
            worker_names=worker_names,
            status="running",
        )

        logger.info("Dummy: Started worker group %s with workers %s", worker_group_id.decode(), worker_names)

        return worker_group_id

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> None:
        """Simulate shutting down a worker group.

        Args:
            worker_group_id: The ID of the worker group to shutdown.

        Raises:
            WorkerGroupNotFoundError: If the worker group doesn't exist.
        """
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist.")

        group_info = self._worker_groups.pop(worker_group_id)
        logger.info("Dummy: Shut down worker group %s (request_id=%s)", worker_group_id.decode(), group_info.request_id)

    async def get_worker_group_status(self, worker_group_id: WorkerGroupID) -> dict:
        """Get simulated status of a worker group.

        Args:
            worker_group_id: The ID of the worker group.

        Returns:
            Status dict mimicking ORB's get_request_status response.
        """
        if worker_group_id not in self._worker_groups:
            return None

        group_info = self._worker_groups[worker_group_id]

        # Simulate ORB's response format
        machines = []
        for name in group_info.worker_names:
            machines.append(
                {
                    "machineId": f"dummy-uid-{name}",
                    "name": name,
                    "result": "succeed",
                    "status": "running",
                    "privateIpAddress": "10.0.0.1",
                    "publicIpAddress": "",
                    "launchtime": "2024-01-01T00:00:00Z",
                    "message": "Simulated by DummyORBWorkerAdapter",
                }
            )

        return {
            "requests": [
                {"requestId": group_info.request_id, "message": "", "status": "complete", "machines": machines}
            ]
        }

    async def webhook_handler(self, request: Request) -> web.Response:
        """Handle webhook requests from the Scaler scheduler."""
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
                    "base_capabilities": {},
                    "adapter_type": "orb-dummy",
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


def main():
    """Run the dummy adapter as a standalone server for testing."""
    import argparse

    parser = argparse.ArgumentParser(description="Dummy ORB Worker Adapter for testing")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")
    parser.add_argument("--max-workers", type=int, default=10, help="Maximum workers")
    parser.add_argument("--workers-per-group", type=int, default=1, help="Workers per group")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    config = DummyORBConfig(
        adapter_web_host=args.host,
        adapter_web_port=args.port,
        max_workers=args.max_workers,
        workers_per_group=args.workers_per_group,
    )

    adapter = DummyORBWorkerAdapter(config)
    app = adapter.create_app()

    logger.info("Starting DummyORBWorkerAdapter on %s:%d", args.host, args.port)
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
