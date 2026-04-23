"""Abstracts the connection between the user-facing ``Client`` and the background
``ClientAgent`` so that different execution environments can share the same
``Client`` code.

On native CPython, the ``Client`` runs in the user's thread and the
``ClientAgent`` runs its asyncio loop in a background thread. The two
communicate over an "internal" IPC socket pair (``ipc://`` or ``tcp://`` on
Windows) so the ``Client`` can submit tasks synchronously while the agent
handles network I/O and manager state on its own loop.

Browser / Pyodide environments cannot start threads and cannot create IPC
sockets, but they can run the same ``ClientAgent`` logic directly on the single
available asyncio loop (via JSPI's ``pyodide.ffi.run_sync`` to keep the
``Client``'s sync API). This module defines the interface both paths must
satisfy, and implements the native path.

The second (in-process / browser) implementation lives alongside this one in
the same package and is selected by ``Client`` based on ``sys.platform``.
"""

from __future__ import annotations

import abc
import threading
import uuid
from typing import Optional

from scaler.client.agent.client_agent import ClientAgent
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.serializer.mixins import Serializer
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import ConnectorRemoteType, NetworkBackend, SyncConnector
from scaler.utility.identifiers import ClientID


class ClientAgentBridge(abc.ABC):
    """Bridges a synchronous ``Client`` to an asynchronous ``ClientAgent``.

    Implementations encapsulate the lifecycle of the agent (start/stop/wait)
    and expose a ``SyncConnector``-compatible handle that delivers messages
    from the ``Client`` to the agent's receive handler.
    """

    @abc.abstractmethod
    def start(self) -> None:
        """Start the agent. Must be called exactly once, before any other method."""

    @abc.abstractmethod
    def get_object_storage_address(self) -> AddressConfig:
        """Block until the object storage address is known and return it.

        Called once after ``start()`` to resolve the address the client will
        use for direct object-storage reads/writes.
        """

    @property
    @abc.abstractmethod
    def connector(self) -> SyncConnector:
        """Return the ``SyncConnector`` the ``Client`` uses to talk to the agent.

        Only valid after ``start()`` and ``get_object_storage_address()`` have
        returned.
        """

    @abc.abstractmethod
    def is_alive(self) -> bool:
        """Return True if the agent is still running."""

    @abc.abstractmethod
    def join(self) -> None:
        """Wait for the agent to fully stop. Safe to call multiple times."""


class IPCAgentBridge(ClientAgentBridge):
    """Native-CPython bridge that runs the ``ClientAgent`` on a background thread.

    Creates a dedicated "internal" connector address, instantiates the agent
    with that address on the bind side, and exposes a ``SyncConnector`` on the
    connect side so the ``Client`` can ``send()``/``receive()`` synchronously
    from the user's thread while the agent's loop runs concurrently.

    This is the historical behavior of ``Client.__initialize__``; the class
    only wraps it behind a common interface so the browser path can supply a
    drop-in replacement.
    """

    def __init__(
        self,
        *,
        identity: ClientID,
        scheduler_address: AddressConfig,
        network_backend: NetworkBackend,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
        object_storage_address: Optional[str] = None,
    ) -> None:
        self._identity = identity
        self._backend = network_backend

        self._client_agent_address = self._backend.create_internal_address(
            f"scaler_client_{uuid.uuid4().hex}", same_process=True
        )

        self._agent = ClientAgent(
            identity=identity,
            client_agent_address=self._client_agent_address,
            scheduler_address=scheduler_address,
            network_backend=network_backend,
            future_manager=future_manager,
            stop_event=stop_event,
            timeout_seconds=timeout_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            serializer=serializer,
            object_storage_address=object_storage_address,
        )

        self._connector: Optional[SyncConnector] = None

    def start(self) -> None:
        self._agent.start()

    def get_object_storage_address(self) -> AddressConfig:
        return self._agent.get_object_storage_address()

    @property
    def connector(self) -> SyncConnector:
        if self._connector is None:
            # Lazily create the sync connector so we don't pay the cost until the
            # agent has reported that the object-storage address is ready (which
            # matches the ordering in the pre-refactor Client.__initialize__).
            self._connector = self._backend.create_sync_connector(
                identity=self._identity,
                connector_remote_type=ConnectorRemoteType.Connector,
                address=self._client_agent_address,
            )
        return self._connector

    def is_alive(self) -> bool:
        return self._agent.is_alive()

    def join(self) -> None:
        self._agent.join()
