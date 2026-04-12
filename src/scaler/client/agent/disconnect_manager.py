from typing import Optional

from scaler.client.agent.mixins import DisconnectManager
from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import ClientDisconnect, ClientShutdownResponse
from scaler.utility.exceptions import ClientQuitException, ClientShutdownException


class ClientDisconnectManager(DisconnectManager):
    def __init__(self):
        self._connector_internal: Optional[AsyncConnector] = None
        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_internal: AsyncConnector, connector_external: AsyncConnector):
        self._connector_internal = connector_internal
        self._connector_external = connector_external

    async def on_client_disconnect(self, disconnect: ClientDisconnect):
        assert self._connector_external is not None
        await self._connector_external.send(disconnect)

        if disconnect.disconnectType == ClientDisconnect.DisconnectType.disconnect:
            raise ClientQuitException("client disconnecting")

    async def on_client_shutdown_response(self, response: ClientShutdownResponse):
        assert self._connector_internal is not None
        await self._connector_internal.send(response)

        raise ClientShutdownException("cluster shutting down")
