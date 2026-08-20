from __future__ import annotations

import logging
from typing import Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.protocol.capnp import WorkerManagerShutdown
from scaler.scheduler.controllers.worker_manager_utilties import build_set_desired_command

logger = logging.getLogger(__name__)


class ChildManagerLink:
    """The link a provisioner-shaped manager holds to the child managers inside its resources.

    A nested unit is a resource running a native worker manager, so this manager commands that
    child rather than any worker. It has two levers: lower the child's target, or retire it. The
    parent destroys the resource only once the child reports that its fleet is gone.
    """

    def __init__(self) -> None:
        self._binder: Optional[AsyncBinder] = None
        self._children_address: Optional[AddressConfig] = None

    def register(self, binder: AsyncBinder, children_address: AddressConfig) -> None:
        self._binder = binder
        self._children_address = children_address

    @property
    def children_address(self) -> Optional[AddressConfig]:
        """The address a child dials, which the start command for the resource must carry."""
        return self._children_address

    async def shutdown(self, unit_id: str) -> bool:
        """Ask the child manager to drain its whole fleet. False if there is no link to it."""
        if self._binder is None:
            return False

        await self._binder.send(unit_id.encode(), WorkerManagerShutdown(), detached=True)
        return True

    async def set_task_concurrency(self, unit_id: str, task_concurrency: int) -> bool:
        """Lower or raise the child's target, which is the lever used before retiring a unit."""
        if self._binder is None:
            return False

        command = build_set_desired_command([({}, task_concurrency)])
        await self._binder.send(unit_id.encode(), command, detached=True)
        return True
