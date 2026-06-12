import collections
import dataclasses
import logging
import queue
import threading
from typing import Dict, Optional

from scaler.client.agent.mixins import ActorManager
from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import ActorCreate, ActorDestroy, ActorMessage, ActorState, ActorStateUpdate
from scaler.utility.exceptions import ActorDeadError, ActorNotFoundError
from scaler.utility.identifiers import ActorID

# placed in an actor's inbox when it dies; re-put on consumption so that every pending and
# future __receive__() call wakes up and raises ActorDeadError
_DEATH_SENTINEL = object()

# dead records are kept so a just-died actor can still be inspected and its inbox drained, but
# they must not accumulate for the client's whole life; the oldest are evicted past this cap
_MAX_RETAINED_DEAD_RECORDS = 1024


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
        # dead actor ids in death order, used to evict the oldest dead records once the retained
        # set exceeds the cap (see __retain_dead); guarded by _dict_lock like the records dict
        self._dead_actor_ids: "collections.OrderedDict[ActorID, None]" = collections.OrderedDict()
        # set once the agent has declared every actor dead on shutdown; any record created after
        # that point is born dead, so a create_actor() racing the shutdown never hangs
        self._no_new_actors = False

        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_external: AsyncConnector):
        self._connector_external = connector_external

    def track_actor(self, actor_id: ActorID):
        self.__ensure_record(actor_id)

    def untrack_actor(self, actor_id: ActorID):
        """Drops a record again, e.g. when a create could not be sent after track_actor()."""
        with self._dict_lock:
            self._actor_id_to_record.pop(actor_id, None)
            self._dead_actor_ids.pop(actor_id, None)

    def get_actor_lifecycle(self, actor_id: ActorID) -> Optional[_Lifecycle]:
        """Returns the actor's current lifecycle snapshot, or None if no record is tracked.

        None and a dead lifecycle are intentionally distinct: None means the actor is unknown
        (never tracked, or its record was purged), which a caller may treat differently from a
        known death.
        """
        record = self.__get_record(actor_id)
        if record is None:
            return None
        return record.lifecycle

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
            raise ActorNotFoundError(f"unknown actor {actor_id!r}")

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

        # the message is already detached from the receive buffer (deserialize copies it), so the
        # payload is a stable bytes object we can hand to the inbox without re-copying
        record.inbox.put(actor_message.payload)

    async def on_actor_state_update(self, actor_state_update: ActorStateUpdate):
        actor_id = ActorID(bytes(actor_state_update.actorId))
        record = self.__get_record(actor_id)
        if record is None:
            # an update for an actor this client never tracked: the scheduler only routes updates
            # for actors we own, so it is unsolicited (e.g. forged); drop it instead of creating
            # a phantom record
            logging.warning(f"{self.__class__.__name__}: dropping state update for untracked actor {actor_id!r}")
            return

        new_state = ActorState(actor_state_update.state.value)
        death_reason = None
        if new_state == ActorState.dead:
            death_reason = ActorStateUpdate.DeathInfo.Reason(actor_state_update.deathInfo.reason.value)

        self.__transition(actor_id, record, new_state, death_reason)

    def set_all_actors_dead(
        self, reason: ActorStateUpdate.DeathInfo.Reason = ActorStateUpdate.DeathInfo.Reason.clientDisconnected
    ):
        with self._dict_lock:
            # latch inside the same locked section that snapshots the records, so a record added
            # by __ensure_record after this point sees the flag and is born dead instead of being
            # missed by the snapshot below
            self._no_new_actors = True
            items = list(self._actor_id_to_record.items())

        for actor_id, record in items:
            self.__transition(actor_id, record, ActorState.dead, reason)

    def __transition(
        self,
        actor_id: ActorID,
        record: _ActorRecord,
        new_state: ActorState,
        death_reason: Optional[ActorStateUpdate.DeathInfo.Reason],
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
            became_dead = new_state == ActorState.dead

        if became_dead:
            # registered outside the condition (the only place that takes _dict_lock while a
            # record lock is held would invert ordering); first death only, since dead is absorbing
            self.__retain_dead(actor_id)

    def __retain_dead(self, actor_id: ActorID):
        with self._dict_lock:
            self._dead_actor_ids[actor_id] = None
            while len(self._dead_actor_ids) > _MAX_RETAINED_DEAD_RECORDS:
                evicted, _ = self._dead_actor_ids.popitem(last=False)
                self._actor_id_to_record.pop(evicted, None)

    def __get_record(self, actor_id: ActorID) -> Optional[_ActorRecord]:
        with self._dict_lock:
            return self._actor_id_to_record.get(actor_id)

    def __ensure_record(self, actor_id: ActorID) -> _ActorRecord:
        with self._dict_lock:
            record = self._actor_id_to_record.setdefault(actor_id, _ActorRecord())
            no_new_actors = self._no_new_actors

        if no_new_actors:
            # the agent already ran its shutdown death loop; transition (outside the dict lock,
            # the design's single-dict-op rule) so this record never lingers as pending
            self.__transition(actor_id, record, ActorState.dead, ActorStateUpdate.DeathInfo.Reason.clientDisconnected)

        return record
