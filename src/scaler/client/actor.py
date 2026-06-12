from typing import Optional

from scaler.client.agent.actor_manager import ClientActorManager
from scaler.io.mixins import SyncConnector
from scaler.protocol.capnp import ActorDestroy, ActorState, ActorStateUpdate
from scaler.utility.identifiers import ActorID, ClientID


class ActorHandle:
    """Handle to a remote actor instance.

    Creation is asynchronous: the handle is returned before the actor is alive. The `state`
    property reflects the latest ActorStateUpdate received from the scheduler.

    Only lifecycle operations are supported for now; RPC-style method calls and streaming are
    not implemented yet.
    """

    def __init__(
        self, actor_id: ActorID, source: ClientID, actor_manager: ClientActorManager, connector_agent: SyncConnector
    ):
        self._actor_id = actor_id
        self._source = source
        self._actor_manager = actor_manager
        self._connector_agent = connector_agent

    def __repr__(self) -> str:
        return f"ActorHandle({self._actor_id.hex()}, state={self.state.name})"

    @property
    def actor_id(self) -> ActorID:
        """Unique identifier for this actor."""
        return self._actor_id

    @property
    def state(self) -> ActorState:
        """Latest known lifecycle state of this actor."""
        return self._actor_manager.get_actor_state(self._actor_id)

    @property
    def dead(self) -> bool:
        """Whether the actor has died."""
        return self.state == ActorState.dead

    @property
    def death_reason(self) -> Optional["ActorStateUpdate.DeathInfo.Reason"]:
        """Why the actor died, or None while it has not."""
        return self._actor_manager.get_actor_death_reason(self._actor_id)

    def wait_for_state(self, state: ActorState, timeout: Optional[float] = None) -> ActorState:
        """Blocks until the actor's lifecycle reaches `state`, without polling.

        States only move forward, so the wait completes once the lifecycle reaches or passes
        `state`; dead satisfies any wait. Returns the state actually reached, which the caller
        should check (e.g. waiting for alive may return dead if creation failed).

        :param state: lifecycle state to wait for
        :param timeout: maximum seconds to wait; None blocks forever
        :raises TimeoutError: if the timeout expires first
        """
        return self._actor_manager.wait_for_actor_state(self._actor_id, state, timeout=timeout)

    def destroy(self, force: bool = False) -> None:
        """Destroy the actor and release its resources.

        Asynchronous and idempotent: the request is sent and the actor transitions to dead once
        the worker confirms.

        :param force: If True, forcefully kill the actor process instead of letting it wind down
        """
        if self.dead:
            return

        mode = ActorDestroy.Mode.kill if force else ActorDestroy.Mode.graceful
        self._connector_agent.send(ActorDestroy(actorId=self._actor_id, source=self._source, mode=mode))
