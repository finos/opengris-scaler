from __future__ import annotations

import logging
from typing import Callable, Dict, Optional, Set

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.protocol.capnp import WorkerShutdown
from scaler.utility.exitcode import describe_exitcode
from scaler.worker_manager_adapter.worker_process import WorkerProcess

logger = logging.getLogger(__name__)


class ProxyWorkerPool:
    """The local proxy worker processes of an HPC-style provisioner.

    An HPC manager's unit is a process on this machine that forwards tasks to a remote service, so
    it commands that process exactly the way a native manager commands a worker. All three HPC
    backends need identical mechanics, and this is the collaborator they share rather than a base
    class they inherit.
    """

    def __init__(self, create_worker: Callable[[], WorkerProcess], description: str) -> None:
        self._create_worker = create_worker
        self._description = description

        self._workers: Dict[str, WorkerProcess] = {}
        self._binder: Optional[AsyncBinder] = None
        self._children_address: Optional[AddressConfig] = None

    def register(self, binder: AsyncBinder, children_address: AddressConfig) -> None:
        self._binder = binder
        self._children_address = children_address

    async def create(self) -> str:
        worker = self._create_worker()
        worker.start()

        self._workers[worker.name] = worker
        logger.info(f"started {self._description} {worker.name!r}")
        return worker.name

    async def destroy(self, unit_id: str) -> None:
        worker = self._workers.pop(unit_id, None)
        if worker is None:
            return

        if worker.is_alive():
            worker.terminate()

        logger.info(f"destroyed {self._description} {unit_id!r}")

    async def shutdown(self, unit_id: str) -> None:
        worker = self._workers.get(unit_id)
        if worker is None or not worker.is_alive():
            return

        if self._binder is not None:
            await self._binder.send(unit_id.encode(), WorkerShutdown(), detached=True)
            return

        # No link to the process: a terminate loses whatever jobs it is tracking.
        worker.terminate()

    async def poll(self) -> Set[str]:
        alive = set()
        for unit_id, worker in list(self._workers.items()):
            if worker.is_alive():
                alive.add(unit_id)
                continue

            worker.join()
            if worker.exitcode == 0:
                logger.info(f"{self._description} {unit_id!r} shut down cleanly")
            else:
                logger.warning(
                    f"{self._description} {unit_id!r} exited unexpectedly "
                    f"(exitcode={describe_exitcode(worker.exitcode)})"
                )
            self._workers.pop(unit_id, None)

        return alive
