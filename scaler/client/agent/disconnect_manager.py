from typing import Optional

from scaler.client.agent.mixins import DisconnectManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import ClientDisconnect, ClientShutdownResponse
from scaler.utility.exceptions import ClientQuitException, ClientShutdownException


class ClientDisconnectManager(DisconnectManager):
    def __init__(self):
        self._connector_internal: Optional[AsyncConnector] = None
        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_internal: AsyncConnector, connector_external: AsyncConnector):
        self._connector_internal = connector_internal
        self._connector_external = connector_external

    async def on_client_disconnect(self, disconnect: ClientDisconnect):
        await self._connector_external.send(disconnect)

        if disconnect.disconnect_type == ClientDisconnect.DisconnectType.Disconnect:
            raise ClientQuitException("client disconnecting")

    async def on_client_shutdown_response(self, response: ClientShutdownResponse):
        await self._connector_internal.send(response)

        raise ClientShutdownException("cluster shutting down")
