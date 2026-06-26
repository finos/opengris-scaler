import logging
from typing import Dict, Optional

from scaler.io.mixins import AsyncBinder
from scaler.protocol.capnp import ActorCreate, ActorDestroy, ActorError, ActorMessage, ActorState, ActorStateUpdate
from scaler.scheduler.controllers.mixins import ActorController, WorkerController
from scaler.utility.identifiers import ActorID, ClientID, WorkerID
from scaler.utility.one_to_many_dict import OneToManyDict


class VanillaActorController(ActorController):
    """Routes actor lifecycle messages between owning clients and hosting workers.

    Every client request is answered, possibly with a dead state, so that no client is left
    blocked.
    """

    def __init__(self) -> None:
        self._binder: Optional[AsyncBinder] = None
        self._worker_controller: Optional[WorkerController] = None

        self._client_to_actor_ids: OneToManyDict[ClientID, ActorID] = OneToManyDict()
        self._worker_to_actor_ids: OneToManyDict[WorkerID, ActorID] = OneToManyDict()
        self._actor_id_to_state: Dict[ActorID, ActorState] = dict()

    def register(self, binder: AsyncBinder, worker_controller: WorkerController) -> None:
        self._binder = binder
        self._worker_controller = worker_controller

    def is_actor_worker(self, worker_id: WorkerID) -> bool:
        return self._worker_to_actor_ids.has_key(worker_id)

    async def on_actor_create(self, client_id: ClientID, actor_create: ActorCreate) -> None:
        actor_id = ActorID(bytes(actor_create.actorId))

        if actor_create.source != client_id:
            logging.error(
                f"{self.__class__.__name__}: ActorCreate source {bytes(actor_create.source)!r} does not match "
                f"sender {client_id!r}, rejecting"
            )
            await self.__send_dead(client_id, actor_id, ActorStateUpdate.DeathInfo.Reason.unknownActor)
            return

        if self._actor_id_to_state.get(actor_id) is not None:
            # idempotent re-send: answer with the current state instead of creating a duplicate
            owner = self._client_to_actor_ids.get_key(actor_id)
            if owner != client_id:
                logging.error(f"{self.__class__.__name__}: actor {actor_id!r} is owned by another client, rejecting")
                await self.__send_dead(client_id, actor_id, ActorStateUpdate.DeathInfo.Reason.unknownActor)
                return

            worker_id = self._worker_to_actor_ids.get_key(actor_id)
            await self.__send_state(client_id, actor_id, worker_id, self._actor_id_to_state[actor_id])
            return

        if len(actor_create.capabilities) > 0:
            logging.warning(
                f"{self.__class__.__name__}: actor {actor_id!r} requests capabilities, but capability aware "
                "actor placement is not supported yet"
            )
            await self.__send_dead(
                client_id,
                actor_id,
                ActorStateUpdate.DeathInfo.Reason.placementFailed,
                detail="capability aware actor placement is not supported yet",
            )
            return

        worker_id = await self._worker_controller.acquire_worker_for_actor()
        if worker_id is None:
            await self.__send_dead(
                client_id, actor_id, ActorStateUpdate.DeathInfo.Reason.placementFailed, detail="no worker available"
            )
            return

        self._client_to_actor_ids.add(client_id, actor_id)
        self._worker_to_actor_ids.add(worker_id, actor_id)
        self._actor_id_to_state[actor_id] = ActorState.pending

        await self.__send_state(client_id, actor_id, None, ActorState.pending)
        await self._binder.send(worker_id, actor_create)

    async def on_actor_destroy(self, client_id: ClientID, actor_destroy: ActorDestroy) -> None:
        actor_id = ActorID(bytes(actor_destroy.actorId))

        if actor_id not in self._actor_id_to_state:
            # destroying an unknown or already dead actor is idempotent: answer so the client
            # never blocks on a destroy that has nothing left to do
            await self.__send_dead(client_id, actor_id, ActorStateUpdate.DeathInfo.Reason.unknownActor)
            return

        owner = self._client_to_actor_ids.get_key(actor_id)
        if owner != client_id or actor_destroy.source != client_id:
            logging.error(
                f"{self.__class__.__name__}: client {client_id!r} requested destroy of actor {actor_id!r} it "
                "does not own, rejecting"
            )
            await self.__send_dead(client_id, actor_id, ActorStateUpdate.DeathInfo.Reason.unknownActor)
            return

        worker_id = self._worker_to_actor_ids.get_key(actor_id)
        await self._binder.send(worker_id, actor_destroy)

    async def on_actor_state_update(self, worker_id: WorkerID, actor_state_update: ActorStateUpdate) -> None:
        actor_id = ActorID(bytes(actor_state_update.actorId))
        state = ActorState(actor_state_update.state.value)

        if actor_id not in self._actor_id_to_state:
            # an update for an actor we do not track (already cleaned up, or forged). We have no
            # authoritative owner to route it to, so drop it rather than trust the wire `source`
            # and let a forged update materialize phantom records on a victim client
            logging.warning(f"{self.__class__.__name__}: dropping state update for untracked actor {actor_id!r}")
            return

        if self._worker_to_actor_ids.has_value(actor_id) and bytes(
            self._worker_to_actor_ids.get_key(actor_id)
        ) != bytes(worker_id):
            logging.error(
                f"{self.__class__.__name__}: worker {worker_id!r} sent a state update for actor {actor_id!r} it "
                "does not host, dropping"
            )
            return

        # route to the owner from our authoritative table, never the wire `source`
        owner = self._client_to_actor_ids.get_key(actor_id)

        # states only move forward; a stale, out-of-order update must not roll the actor back
        # (mirrors the client-side monotonic guard). dead still forgets the actor below.
        if state < self._actor_id_to_state[actor_id]:
            logging.warning(
                f"{self.__class__.__name__}: ignoring stale state {state.name} for actor {actor_id!r} "
                f"(current {self._actor_id_to_state[actor_id].name})"
            )
            return

        self._actor_id_to_state[actor_id] = state

        if state == ActorState.dead:
            self.__forget_actor(actor_id)

        await self._binder.send(owner, actor_state_update)

    async def on_actor_message(self, source: bytes, actor_message: ActorMessage) -> None:
        actor_id = ActorID(bytes(actor_message.actorId))

        if source.startswith(b"Client|"):
            # client -> actor direction: only the actor's owner may talk to it. Validate against
            # our authoritative table, not the wire `source` the sender chose.
            claimed_owner = bytes(actor_message.source)
            if source != claimed_owner or (
                self._client_to_actor_ids.has_value(actor_id)
                and bytes(self._client_to_actor_ids.get_key(actor_id)) != source
            ):
                logging.error(
                    f"{self.__class__.__name__}: client {source!r} sent a message for actor {actor_id!r} it "
                    "does not own, dropping"
                )
                return

            if not self._worker_to_actor_ids.has_value(actor_id):
                # the actor is dead or unknown; the owner learns that from the actor's state
                # updates, so the message is dropped silently by design
                logging.warning(f"{self.__class__.__name__}: dropping message for unknown actor {actor_id!r}")
                return

            await self._binder.send(self._worker_to_actor_ids.get_key(actor_id), actor_message)
            return

        # worker -> client direction: route to the owner from our authoritative table (never the
        # wire `source`), and verify the sender actually hosts the actor
        if not self._worker_to_actor_ids.has_value(actor_id):
            logging.warning(f"{self.__class__.__name__}: dropping message for untracked actor {actor_id!r}")
            return

        if bytes(self._worker_to_actor_ids.get_key(actor_id)) != source:
            logging.error(
                f"{self.__class__.__name__}: worker {source!r} sent a message for actor {actor_id!r} it does "
                "not host, dropping"
            )
            return

        await self._binder.send(self._client_to_actor_ids.get_key(actor_id), actor_message)

    async def on_client_disconnect(self, client_id: ClientID) -> None:
        if client_id not in self._client_to_actor_ids.keys():
            return

        actor_ids = self._client_to_actor_ids.get_values(client_id).copy()
        logging.info(f"{self.__class__.__name__}: destroying {len(actor_ids)} actor(s) of client {client_id!r}")
        for actor_id in actor_ids:
            if not self._worker_to_actor_ids.has_value(actor_id):
                # a concurrent handler (e.g. a worker disconnect during the await below) already
                # forgot this actor; skip it rather than fail the get_key lookup
                continue
            worker_id = self._worker_to_actor_ids.get_key(actor_id)
            self.__forget_actor(actor_id)
            await self._binder.send(
                worker_id, ActorDestroy(actorId=actor_id, source=client_id, mode=ActorDestroy.Mode.kill)
            )

    async def on_worker_disconnect(self, worker_id: WorkerID) -> None:
        if worker_id not in self._worker_to_actor_ids.keys():
            return

        actor_ids = self._worker_to_actor_ids.get_values(worker_id).copy()
        logging.info(f"{self.__class__.__name__}: {len(actor_ids)} actor(s) died with worker {worker_id!r}")
        for actor_id in actor_ids:
            if not self._client_to_actor_ids.has_value(actor_id):
                # a concurrent handler (e.g. a client disconnect during the await below) already
                # forgot this actor; skip it rather than fail the get_key lookup
                continue
            owner = self._client_to_actor_ids.get_key(actor_id)
            self.__forget_actor(actor_id)
            await self.__send_dead(owner, actor_id, ActorStateUpdate.DeathInfo.Reason.workerDied, worker_id=worker_id)

    def __forget_actor(self, actor_id: ActorID) -> None:
        self._actor_id_to_state.pop(actor_id, None)
        if self._client_to_actor_ids.has_value(actor_id):
            self._client_to_actor_ids.remove_value(actor_id)
        if self._worker_to_actor_ids.has_value(actor_id):
            self._worker_to_actor_ids.remove_value(actor_id)

    async def __send_state(
        self, client_id: ClientID, actor_id: ActorID, worker_id: Optional[WorkerID], state: ActorState
    ) -> None:
        await self._binder.send(
            client_id, ActorStateUpdate(actorId=actor_id, source=client_id, workerId=worker_id or b"", state=state)
        )

    async def __send_dead(
        self,
        client_id: ClientID,
        actor_id: ActorID,
        reason: ActorStateUpdate.DeathInfo.Reason,
        detail: str = "",
        worker_id: Optional[WorkerID] = None,
    ) -> None:
        error = (
            ActorError(errorType="scaler.utility.exceptions.ActorDeadError", message=detail) if detail else ActorError()
        )
        await self._binder.send(
            client_id,
            ActorStateUpdate(
                actorId=actor_id,
                source=client_id,
                workerId=worker_id or b"",
                state=ActorState.dead,
                deathInfo=ActorStateUpdate.DeathInfo(reason=reason, error=error),
            ),
        )
