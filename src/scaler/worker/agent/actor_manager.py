import asyncio
import dataclasses
import logging
from typing import Optional, Set

from scaler.io.mixins import AsyncBinder, AsyncConnector
from scaler.protocol.capnp import ActorCreate, ActorDestroy, ActorError, ActorMessage, ActorState, ActorStateUpdate
from scaler.utility.identifiers import ActorID, ProcessorID, WorkerID
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import ActorManager, ProcessorManager

_PROCESSOR_READY_WAIT_SECONDS = 30
_PROCESSOR_READY_POLL_SECONDS = 0.1


@dataclasses.dataclass
class _ActorDesignation:
    actor_id: ActorID
    source: bytes
    processor_id: ProcessorID
    alive: bool = False  # the actor reported state alive (constructor finished)
    destroy_requested: bool = False


class VanillaActorManager(Looper, ActorManager):
    """Relays the actor designation of this worker.

    A worker is designated as an actor by the scheduler; being an actor is a response mode of
    the worker's existing processor, not a separate process. This manager forwards ActorCreate,
    ActorDestroy and ActorMessage to the current processor, stamps the worker identity on the
    processor's ActorStateUpdate reports, and detects the processor exiting underneath the
    designation. Process lifecycle stays with the processor manager.
    """

    def __init__(self, identity: WorkerID):
        self._identity = identity

        self._connector_external: Optional[AsyncConnector] = None
        self._binder_internal: Optional[AsyncBinder] = None
        self._processor_manager: Optional[ProcessorManager] = None

        self._designation: Optional[_ActorDesignation] = None
        self._pending_creates: Set[asyncio.Task] = set()

    def register(
        self, connector_external: AsyncConnector, binder_internal: AsyncBinder, processor_manager: ProcessorManager
    ):
        self._connector_external = connector_external
        self._binder_internal = binder_internal
        self._processor_manager = processor_manager

    async def on_actor_create(self, actor_create: ActorCreate):
        try:
            await self.__on_actor_create(actor_create)
        except Exception:
            logging.exception(f"{self.__class__.__name__}: failed to handle ActorCreate:")

    async def on_actor_destroy(self, actor_destroy: ActorDestroy):
        try:
            await self.__on_actor_destroy(actor_destroy)
        except Exception:
            logging.exception(f"{self.__class__.__name__}: failed to handle ActorDestroy:")

    async def on_actor_state_update(self, host_identity: bytes, actor_state_update: ActorStateUpdate):
        designation = self._designation
        actor_id = ActorID(bytes(actor_state_update.actorId))
        state = ActorState(actor_state_update.state.value)

        if designation is not None and designation.actor_id == actor_id:
            if state == ActorState.alive:
                designation.alive = True
            elif state == ActorState.dead:
                self._designation = None  # the designation is released; heartbeats rejoin the pool

        await self._connector_external.send(self.__stamp_worker_id(actor_state_update))

    async def on_actor_message(self, actor_message: ActorMessage):
        designation = self._designation

        if (
            designation is None
            or designation.actor_id != ActorID(bytes(actor_message.actorId))
            or not designation.alive
        ):
            # dead, unknown or not-yet-alive actor: drop; the owner learns the actor fate from
            # its state updates
            logging.warning(f"{self.__class__.__name__}: dropping message for actor {bytes(actor_message.actorId)!r}")
            return

        await self._binder_internal.send(designation.processor_id, actor_message)

    async def on_actor_message_from_host(self, actor_message: ActorMessage):
        await self._connector_external.send(actor_message)

    async def routine(self):
        designation = self._designation
        if designation is None:
            return

        if self._processor_manager.current_processor_id() == designation.processor_id:
            return

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

    def destroy(self, reason: str):
        for task in self._pending_creates:
            task.cancel()

        self._designation = None

    async def __on_actor_create(self, actor_create: ActorCreate):
        actor_id = ActorID(bytes(actor_create.actorId))

        designation = self._designation
        if designation is not None:
            if designation.actor_id == actor_id:
                # idempotent re-send: report the current state instead of designating twice
                state = ActorState.alive if designation.alive else ActorState.creating
                await self.__send_state(actor_id, designation.source, state)
                return

            logging.error(f"{self.__class__.__name__}: already designated as another actor, rejecting {actor_id!r}")
            await self.__send_dead(
                actor_id,
                bytes(actor_create.source),
                ActorStateUpdate.DeathInfo.Reason.unknownActor,
                "worker is already designated as another actor",
            )
            return

        if self._processor_manager.current_processor_id() is not None:
            await self.__designate(actor_create)
            return

        # the processor initializes shortly after the worker starts; an ActorCreate can only
        # beat it by a tiny window, so wait for it off the message loop
        task = asyncio.get_event_loop().create_task(self.__designate_when_processor_ready(actor_create.to_bytes()))
        self._pending_creates.add(task)
        task.add_done_callback(self._pending_creates.discard)

    async def __designate_when_processor_ready(self, actor_create_payload: bytes):
        actor_create = ActorCreate.from_bytes(actor_create_payload)

        waited_seconds = 0.0
        while self._processor_manager.current_processor_id() is None:
            if waited_seconds >= _PROCESSOR_READY_WAIT_SECONDS:
                await self.__send_dead(
                    ActorID(bytes(actor_create.actorId)),
                    bytes(actor_create.source),
                    ActorStateUpdate.DeathInfo.Reason.constructorFailed,
                    "worker has no initialized processor",
                )
                return

            await asyncio.sleep(_PROCESSOR_READY_POLL_SECONDS)
            waited_seconds += _PROCESSOR_READY_POLL_SECONDS

        if self._designation is None:
            await self.__designate(actor_create)

    async def __designate(self, actor_create: ActorCreate):
        actor_id = ActorID(bytes(actor_create.actorId))
        source = bytes(actor_create.source)
        processor_id = self._processor_manager.current_processor_id()

        self._designation = _ActorDesignation(actor_id=actor_id, source=source, processor_id=processor_id)
        logging.info(f"{self.__class__.__name__}: worker designated as actor {actor_id!r}")

        await self.__send_state(actor_id, source, ActorState.creating)
        await self._binder_internal.send(processor_id, actor_create)

    async def __on_actor_destroy(self, actor_destroy: ActorDestroy):
        actor_id = ActorID(bytes(actor_destroy.actorId))

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

    def __stamp_worker_id(self, actor_state_update: ActorStateUpdate) -> ActorStateUpdate:
        death_info = actor_state_update.deathInfo
        return ActorStateUpdate(
            actorId=bytes(actor_state_update.actorId),
            source=bytes(actor_state_update.source),
            workerId=self._identity,
            state=ActorState(actor_state_update.state.value),
            deathInfo=ActorStateUpdate.DeathInfo(
                reason=ActorStateUpdate.DeathInfo.Reason(death_info.reason.value),
                error=ActorError(
                    errorType=death_info.error.errorType,
                    message=death_info.error.message,
                    traceback=death_info.error.traceback,
                    serialized=bytes(death_info.error.serialized),
                ),
            ),
        )

    async def __send_state(self, actor_id: ActorID, source: bytes, state: ActorState):
        await self._connector_external.send(
            ActorStateUpdate(actorId=actor_id, source=source, workerId=self._identity, state=state)
        )

    async def __send_dead(
        self, actor_id: ActorID, source: bytes, reason: "ActorStateUpdate.DeathInfo.Reason", detail: str
    ):
        error = ActorError(errorType="scaler.ActorDiedError", message=detail) if detail else ActorError()
        await self._connector_external.send(
            ActorStateUpdate(
                actorId=actor_id,
                source=source,
                workerId=self._identity,
                state=ActorState.dead,
                deathInfo=ActorStateUpdate.DeathInfo(reason=reason, error=error),
            )
        )
