import dataclasses
import logging
import queue
import threading
from typing import Dict, Optional

from scaler.client.agent.mixins import ActorManager
from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import ActorCreate, ActorDestroy, ActorMessage, ActorState, ActorStateUpdate
from scaler.utility.exceptions import ActorDeadError
from scaler.utility.identifiers import ActorID

# placed in an actor's inbox when it dies; re-put on consumption so that every pending and
# future __receive__() call wakes up and raises ActorDeadError
_DEATH_SENTINEL = object()


@dataclasses.dataclass(frozen=True)
class _Lifecycle:
    """Immutable lifecycle snapshot; replaced wholesale on transition, never mutated.

    Any reader holding a record takes one reference and gets a (state, death_reason) pair that
    genuinely coexisted, with no lock and regardless of when the fields are read.
    """

    state: ActorState
    death_reason: Optional[ActorStateUpdate.DeathInfo.Reason]


class _ActorRecord:
    def __init__(self):
        # serializes lifecycle transitions and carries the wait/notify protocol; plain reads
        # of `lifecycle` never take it
        self.condition = threading.Condition()
        self.lifecycle = _Lifecycle(state=ActorState.pending, death_reason=None)
        self.inbox: queue.Queue = queue.Queue()


class ClientActorManager(ActorManager):
    """Client-side actor records, shared between the user thread and the agent thread.

    Concurrency design: the records dict is the only structure guarded by the manager's lock,
    and every locked section is a single dict operation. Record values are safe to use outside
    the lock by construction: lifecycle is an immutable snapshot, the inbox is a synchronized
    queue, and blocking waits go through the record's condition variable instead of polling.
    """

    def __init__(self):
        self._dict_lock = threading.Lock()
        self._actor_id_to_record: Dict[ActorID, _ActorRecord] = dict()

        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_external: AsyncConnector):
        self._connector_external = connector_external

    def track_actor(self, actor_id: ActorID):
        self.__ensure_record(actor_id)

    def get_actor_state(self, actor_id: ActorID) -> ActorState:
        record = self.__get_record(actor_id)
        if record is None:
            return ActorState.dead
        return record.lifecycle.state

    def get_actor_death_reason(self, actor_id: ActorID) -> Optional[ActorStateUpdate.DeathInfo.Reason]:
        record = self.__get_record(actor_id)
        if record is None:
            return None
        return record.lifecycle.death_reason

    def wait_for_actor_state(self, actor_id: ActorID, state: ActorState, timeout: Optional[float] = None) -> ActorState:
        """Blocks the calling (user) thread until the actor's lifecycle reaches `state`.

        States only move forward (pending < creating < alive < stopping < dead), so the wait
        completes once the lifecycle reaches or passes `state`; dead satisfies any wait. The
        state actually reached is returned.

        :raises TimeoutError: if the timeout expires first
        """
        record = self.__get_record(actor_id)
        if record is None:
            return ActorState.dead

        with record.condition:
            if not record.condition.wait_for(lambda: record.lifecycle.state >= state, timeout):
                raise TimeoutError(f"actor {actor_id!r} did not reach state {state.name} within {timeout} seconds")
            return record.lifecycle.state

    def receive_actor_message(self, actor_id: ActorID, timeout: Optional[float] = None) -> bytes:
        """Blocks the calling (user) thread until the actor pushes a payload.

        Payloads that arrived before the actor died remain consumable; once drained, a dead
        actor's inbox always raises ActorDeadError.
        """
        record = self.__get_record(actor_id)
        if record is None:
            raise ActorDeadError(f"unknown actor {actor_id!r}")

        try:
            item = record.inbox.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(f"no message from actor {actor_id!r} within {timeout} seconds")

        if item is _DEATH_SENTINEL:
            record.inbox.put(item)
            raise ActorDeadError(f"actor {actor_id!r} is dead: {record.lifecycle.death_reason}")

        return item

    async def on_create_actor(self, actor_create: ActorCreate):
        self.track_actor(ActorID(bytes(actor_create.actorId)))
        await self._connector_external.send(actor_create)

    async def on_destroy_actor(self, actor_destroy: ActorDestroy):
        await self._connector_external.send(actor_destroy)

    async def on_send_actor_message(self, actor_message: ActorMessage):
        await self._connector_external.send(actor_message)

    async def on_actor_message(self, actor_message: ActorMessage):
        actor_id = ActorID(bytes(actor_message.actorId))

        record = self.__get_record(actor_id)
        if record is None:
            logging.warning(f"{self.__class__.__name__}: dropping message for unknown actor {actor_id!r}")
            return

        record.inbox.put(bytes(actor_message.payload))

    async def on_actor_state_update(self, actor_state_update: ActorStateUpdate):
        record = self.__ensure_record(ActorID(bytes(actor_state_update.actorId)))

        new_state = ActorState(actor_state_update.state.value)
        death_reason = None
        if new_state == ActorState.dead:
            death_reason = ActorStateUpdate.DeathInfo.Reason(actor_state_update.deathInfo.reason.value)

        self.__transition(record, new_state, death_reason)

    def set_all_actors_dead(self):
        with self._dict_lock:
            records = list(self._actor_id_to_record.values())

        for record in records:
            self.__transition(record, ActorState.dead, ActorStateUpdate.DeathInfo.Reason.clientDisconnected)

    @staticmethod
    def __transition(
        record: _ActorRecord, new_state: ActorState, death_reason: Optional[ActorStateUpdate.DeathInfo.Reason]
    ):
        with record.condition:
            current = record.lifecycle

            # states only move forward, and dead is absorbing (the first death verdict wins);
            # stale or duplicated updates never roll an actor back
            if new_state < current.state or current.state == ActorState.dead:
                return

            record.lifecycle = _Lifecycle(state=new_state, death_reason=death_reason)

            if new_state == ActorState.dead:
                record.inbox.put(_DEATH_SENTINEL)

            record.condition.notify_all()

    def __get_record(self, actor_id: ActorID) -> Optional[_ActorRecord]:
        with self._dict_lock:
            return self._actor_id_to_record.get(actor_id)

    def __ensure_record(self, actor_id: ActorID) -> _ActorRecord:
        with self._dict_lock:
            return self._actor_id_to_record.setdefault(actor_id, _ActorRecord())
