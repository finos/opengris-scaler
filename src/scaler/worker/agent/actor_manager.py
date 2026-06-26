import asyncio
import dataclasses
import functools
import logging
from typing import Dict, Optional

from scaler.io.mixins import AsyncBinder, AsyncConnector
from scaler.protocol.capnp import ActorCreate, ActorDestroy, ActorError, ActorMessage, ActorState, ActorStateUpdate
from scaler.utility.identifiers import ActorID, ProcessorID, WorkerID
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import ActorManager, ProcessorManager

_PROCESSOR_READY_WAIT_SECONDS = 30


@dataclasses.dataclass
class _ActorDesignation:
    actor_id: ActorID
    source: bytes
    processor_id: ProcessorID
    # the actor's last reported lifecycle state; starts at creating (the constructor has not
    # finished yet) and advances to alive once it does. dead is never stored here -- it releases
    # the designation instead.
    state: ActorState = ActorState.creating
    destroy_requested: bool = False


class VanillaActorManager(Looper, ActorManager):
    """Relays the actor designation of this worker.

    A worker is designated as an actor by the scheduler; being an actor is a response mode of
    the worker's existing processor, not a separate process. This manager forwards ActorCreate,
    ActorDestroy and ActorMessage to the current processor, stamps the worker identity on the
    processor's ActorStateUpdate reports, and detects the processor exiting underneath the
    designation. Process lifecycle stays with the processor manager.
    """

    def __init__(self, identity: WorkerID) -> None:
        self._identity = identity

        self._connector_external: Optional[AsyncConnector] = None
        self._binder_internal: Optional[AsyncBinder] = None
        self._processor_manager: Optional[ProcessorManager] = None

        self._designation: Optional[_ActorDesignation] = None
        # creates still waiting for the processor to initialize, keyed by actor id so a destroy
        # can cancel the matching one before it ever constructs the actor
        self._pending_creates: Dict[ActorID, asyncio.Task] = dict()

    def register(
        self, connector_external: AsyncConnector, binder_internal: AsyncBinder, processor_manager: ProcessorManager
    ) -> None:
        self._connector_external = connector_external
        self._binder_internal = binder_internal
        self._processor_manager = processor_manager

    async def on_actor_state_update(self, host_identity: bytes, actor_state_update: ActorStateUpdate) -> None:
        designation = self._designation
        actor_id = ActorID(bytes(actor_state_update.actorId))
        state = ActorState(actor_state_update.state.value)

        if designation is not None and designation.actor_id == actor_id:
            if state == ActorState.dead:
                self._designation = None  # the designation is released; heartbeats rejoin the pool
            else:
                designation.state = state

        await self._connector_external.send(self.__stamp_worker_id(actor_state_update))

    async def on_actor_message(self, actor_message: ActorMessage) -> None:
        designation = self._designation

        if (
            designation is None
            or designation.actor_id != ActorID(bytes(actor_message.actorId))
            or designation.state != ActorState.alive
        ):
            # dead, unknown or not-yet-alive actor: drop; the owner learns the actor fate from
            # its state updates
            logging.warning(f"{self.__class__.__name__}: dropping message for actor {bytes(actor_message.actorId)!r}")
            return

        await self._binder_internal.send(designation.processor_id, actor_message)

    async def on_actor_message_from_host(self, actor_message: ActorMessage) -> None:
        await self._connector_external.send(actor_message)

    async def routine(self) -> None:
        designation = self._designation
        if designation is None:
            return

        if self._processor_manager.current_processor_id() != designation.processor_id:
            # the processor hosting the actor is gone (it crashed, or was restarted underneath the
            # designation); report the death and release the designation
            self._designation = None

            if designation.destroy_requested:
                reason = ActorStateUpdate.DeathInfo.Reason.destroyed
                detail = ""
            else:
                reason = ActorStateUpdate.DeathInfo.Reason.actorCrashed
                detail = "actor processor exited unexpectedly"
                logging.warning(f"{self.__class__.__name__}: actor {designation.actor_id!r}: {detail}")

            await self.__send_dead(designation.actor_id, designation.source, reason, detail)

    def destroy(self, reason: str) -> None:
        for task in self._pending_creates.values():
            task.cancel()
        self._pending_creates.clear()

        self._designation = None

    async def on_actor_create(self, actor_create: ActorCreate) -> None:
        actor_id = ActorID(bytes(actor_create.actorId))
        source = bytes(actor_create.source)

        designation = self._designation
        if designation is not None:
            if designation.actor_id == actor_id:
                # idempotent re-send: report the current state instead of designating twice
                await self.__send_state(actor_id, designation.source, designation.state)
                return

            logging.error(f"{self.__class__.__name__}: already designated as another actor, rejecting {actor_id!r}")
            await self.__send_dead(
                actor_id,
                source,
                ActorStateUpdate.DeathInfo.Reason.unknownActor,
                "worker is already designated as another actor",
            )
            return

        if actor_id in self._pending_creates:
            # a create for this actor is still waiting for the processor; answer the re-send with
            # the current (creating) state instead of scheduling a second wait
            await self.__send_state(actor_id, source, ActorState.creating)
            return

        if self._pending_creates:
            # one worker hosts at most one actor; another create is already waiting for the
            # processor to initialize, so reject this one
            logging.error(f"{self.__class__.__name__}: already designating another actor, rejecting {actor_id!r}")
            await self.__send_dead(
                actor_id,
                source,
                ActorStateUpdate.DeathInfo.Reason.unknownActor,
                "worker is already designated as another actor",
            )
            return

        if self._processor_manager.current_processor_id() is not None:
            await self.__designate(actor_create)
            return

        # the processor initializes shortly after the worker starts; an ActorCreate can only
        # beat it by a tiny window, so wait for it off the message loop. The message is already
        # detached from the receive buffer, so holding it in the task is safe.
        task = asyncio.get_running_loop().create_task(self.__designate_when_processor_ready(actor_create))
        self._pending_creates[actor_id] = task
        task.add_done_callback(functools.partial(self.__forget_pending_create, actor_id))

    async def on_actor_destroy(self, actor_destroy: ActorDestroy) -> None:
        actor_id = ActorID(bytes(actor_destroy.actorId))

        pending = self._pending_creates.pop(actor_id, None)
        if pending is not None:
            # the create is still waiting for the processor: cancel it so the actor is never
            # constructed. Otherwise the "destroyed" actor would still come alive afterwards and
            # keep the worker silently out of the task pool while hosting it.
            pending.cancel()
            await self.__send_dead(
                actor_id, bytes(actor_destroy.source), ActorStateUpdate.DeathInfo.Reason.destroyed, ""
            )
            return

        designation = self._designation
        if designation is None or designation.actor_id != actor_id:
            # unknown or already released actor: answer so the destroy is idempotent and the
            # owner never blocks
            await self.__send_dead(
                actor_id, bytes(actor_destroy.source), ActorStateUpdate.DeathInfo.Reason.unknownActor, ""
            )
            return

        designation.destroy_requested = True

        if actor_destroy.mode == ActorDestroy.Mode.kill:
            # hard kill works without the actor cooperation (e.g. a wedged constructor); the
            # processor manager restarts a fresh processor and the designation is released here
            self._designation = None
            self._processor_manager.restart_current_processor("actor destroyed (kill)")
            await self.__send_dead(actor_id, designation.source, ActorStateUpdate.DeathInfo.Reason.destroyed, "")
            return

        # graceful: the processor reports dead(destroyed) itself and exits; the processor
        # manager detects the exit and starts a fresh processor
        await self._binder_internal.send(designation.processor_id, actor_destroy)

    def __forget_pending_create(self, actor_id: ActorID, task: asyncio.Task) -> None:
        # only drop the entry if it is still this task: a destroy may have already removed it (and
        # scheduled a fresh one is impossible here since actor ids are never reused)
        if self._pending_creates.get(actor_id) is task:
            del self._pending_creates[actor_id]

    async def __designate_when_processor_ready(self, actor_create: ActorCreate) -> None:
        actor_id = ActorID(bytes(actor_create.actorId))
        source = bytes(actor_create.source)

        try:
            # wait for the processor to report ProcessorInitialized instead of polling for it
            await asyncio.wait_for(
                self._processor_manager.wait_until_current_processor_initialized(),
                timeout=_PROCESSOR_READY_WAIT_SECONDS,
            )
        except asyncio.TimeoutError:
            self._pending_creates.pop(actor_id, None)
            await self.__send_dead(
                actor_id,
                source,
                ActorStateUpdate.DeathInfo.Reason.constructorFailed,
                "worker has no initialized processor",
            )
            return

        # claim the designation atomically with leaving the pending map: there is no await in
        # between, so a destroy that cancelled us removed our entry and this coroutine was
        # cancelled before reaching here. If the entry is already gone, a destroy won the race.
        if self._pending_creates.pop(actor_id, None) is None:
            return

        await self.__designate(actor_create)

    async def __designate(self, actor_create: ActorCreate) -> None:
        actor_id = ActorID(bytes(actor_create.actorId))
        source = bytes(actor_create.source)
        processor_id = self._processor_manager.current_processor_id()

        self._designation = _ActorDesignation(actor_id=actor_id, source=source, processor_id=processor_id)
        logging.info(f"{self.__class__.__name__}: worker designated as actor {actor_id!r}")

        await self.__send_state(actor_id, source, ActorState.creating)
        await self._binder_internal.send(processor_id, actor_create)

    def __stamp_worker_id(self, actor_state_update: ActorStateUpdate) -> ActorStateUpdate:
        state = ActorState(actor_state_update.state.value)
        fields = dict(
            actorId=bytes(actor_state_update.actorId),
            source=bytes(actor_state_update.source),
            workerId=self._identity,
            state=state,
        )

        # deathInfo is only meaningful when the actor is dead; copying it for live-actor updates
        # would put a default DeathInfo (reason=destroyed, empty error) on the wire
        if state == ActorState.dead:
            death_info = actor_state_update.deathInfo
            fields["deathInfo"] = ActorStateUpdate.DeathInfo(
                reason=ActorStateUpdate.DeathInfo.Reason(death_info.reason.value),
                error=ActorError(errorType=death_info.error.errorType, message=death_info.error.message),
            )

        return ActorStateUpdate(**fields)

    async def __send_state(self, actor_id: ActorID, source: bytes, state: ActorState) -> None:
        await self._connector_external.send(
            ActorStateUpdate(actorId=actor_id, source=source, workerId=self._identity, state=state)
        )

    async def __send_dead(
        self, actor_id: ActorID, source: bytes, reason: ActorStateUpdate.DeathInfo.Reason, detail: str
    ) -> None:
        error = (
            ActorError(errorType="scaler.utility.exceptions.ActorDeadError", message=detail) if detail else ActorError()
        )
        await self._connector_external.send(
            ActorStateUpdate(
                actorId=actor_id,
                source=source,
                workerId=self._identity,
                state=ActorState.dead,
                deathInfo=ActorStateUpdate.DeathInfo(reason=reason, error=error),
            )
        )
