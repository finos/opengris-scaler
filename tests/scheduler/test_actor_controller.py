import unittest
from typing import List
from unittest.mock import AsyncMock

from scaler.protocol.capnp import ActorArguments, ActorCreate, ActorDestroy, ActorMessage, ActorState, ActorStateUpdate
from scaler.scheduler.controllers.actor_controller import VanillaActorController
from scaler.utility.identifiers import ActorID, ClientID, WorkerID


def _make_create(actor_id: ActorID, source: ClientID) -> ActorCreate:
    return ActorCreate(
        actorId=actor_id,
        source=source,
        classObjectId=b"",
        constructorArguments=ActorArguments(positional=[], keyword=[]),
        capabilities=[],
    )


class TestActorControllerRouting(unittest.IsolatedAsyncioTestCase):
    """Unit tests for VanillaActorController routing and validation.

    These cover the death reasons and spoofing rejections that are awkward to reproduce against a
    real cluster (workerDied, unknownActor, cross-client message spoofing), and the guarantee that
    routing always follows the scheduler's authoritative tables rather than the attacker-chosen
    wire `source`.
    """

    def setUp(self) -> None:
        self.binder = AsyncMock()
        self.worker_controller = AsyncMock()
        self.controller = VanillaActorController()
        self.controller.register(self.binder, self.worker_controller)

    async def _place_actor(self, client_id: ClientID, worker_id: WorkerID, actor_id: ActorID) -> None:
        self.worker_controller.acquire_worker_for_actor.return_value = worker_id
        await self.controller.on_actor_create(client_id, _make_create(actor_id, client_id))
        self.binder.reset_mock()

    def _sent_to(self, destination: bytes) -> List[object]:
        return [call.args[1] for call in self.binder.send.call_args_list if call.args[0] == destination]

    def _state_updates_to(self, destination: bytes) -> List[ActorStateUpdate]:
        return [message for message in self._sent_to(destination) if isinstance(message, ActorStateUpdate)]

    def _assert_dead(self, update: ActorStateUpdate, reason: "ActorStateUpdate.DeathInfo.Reason") -> None:
        self.assertEqual(ActorState(update.state.value), ActorState.dead)
        self.assertEqual(update.deathInfo.reason.value, reason.value)

    async def test_destroy_unknown_actor_answers_dead_unknown(self) -> None:
        client = ClientID.generate_client_id()
        actor_id = ActorID.generate_actor_id()

        await self.controller.on_actor_destroy(
            client, ActorDestroy(actorId=actor_id, source=client, mode=ActorDestroy.Mode.graceful)
        )

        updates = self._state_updates_to(client)
        self.assertEqual(len(updates), 1)
        self._assert_dead(updates[0], ActorStateUpdate.DeathInfo.Reason.unknownActor)

    async def test_create_with_mismatched_source_rejected(self) -> None:
        client = ClientID.generate_client_id()
        other = ClientID.generate_client_id()
        actor_id = ActorID.generate_actor_id()

        # the sender claims a source that is not its own identity
        await self.controller.on_actor_create(client, _make_create(actor_id, other))

        updates = self._state_updates_to(client)
        self.assertEqual(len(updates), 1)
        self._assert_dead(updates[0], ActorStateUpdate.DeathInfo.Reason.unknownActor)
        self.worker_controller.acquire_worker_for_actor.assert_not_called()

    async def test_create_for_actor_owned_by_other_client_rejected(self) -> None:
        owner = ClientID.generate_client_id()
        intruder = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, worker, actor_id)

        await self.controller.on_actor_create(intruder, _make_create(actor_id, intruder))

        updates = self._state_updates_to(intruder)
        self.assertEqual(len(updates), 1)
        self._assert_dead(updates[0], ActorStateUpdate.DeathInfo.Reason.unknownActor)
        # the owner's actor is untouched
        self.assertTrue(self.controller.is_actor_worker(worker))

    async def test_worker_disconnect_notifies_owner_with_worker_died(self) -> None:
        owner = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, worker, actor_id)

        await self.controller.on_worker_disconnect(worker)

        updates = self._state_updates_to(owner)
        self.assertEqual(len(updates), 1)
        self._assert_dead(updates[0], ActorStateUpdate.DeathInfo.Reason.workerDied)
        self.assertFalse(self.controller.is_actor_worker(worker))

    async def test_cross_client_message_with_spoofed_source_dropped(self) -> None:
        owner = ClientID.generate_client_id()
        intruder = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, worker, actor_id)

        # the intruder forges the owner's identity in the message body; the transport identity
        # (its own) does not match, so the message must be dropped, never forwarded to the worker
        message = ActorMessage(actorId=actor_id, source=owner, payload=b"x")
        await self.controller.on_actor_message(bytes(intruder), message)

        self.assertEqual(self._sent_to(worker), [])

    async def test_message_for_unowned_actor_dropped(self) -> None:
        owner = ClientID.generate_client_id()
        intruder = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, worker, actor_id)

        message = ActorMessage(actorId=actor_id, source=intruder, payload=b"x")
        await self.controller.on_actor_message(bytes(intruder), message)

        self.assertEqual(self._sent_to(worker), [])

    async def test_state_update_for_untracked_actor_dropped(self) -> None:
        victim = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        # a forged update for an actor the scheduler never tracked must not be relayed (it would
        # spawn a phantom record on the victim client)
        update = ActorStateUpdate(actorId=actor_id, source=victim, workerId=worker, state=ActorState.alive)
        await self.controller.on_actor_state_update(worker, update)

        self.assertEqual(self._sent_to(victim), [])

    async def test_state_update_routed_to_authoritative_owner(self) -> None:
        owner = ClientID.generate_client_id()
        attacker = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, worker, actor_id)

        # the hosting worker reports a state update but forges the source field; routing must
        # follow the scheduler's table (owner), not the wire source (attacker)
        update = ActorStateUpdate(actorId=actor_id, source=attacker, workerId=worker, state=ActorState.alive)
        await self.controller.on_actor_state_update(worker, update)

        self.assertEqual(len(self._state_updates_to(owner)), 1)
        self.assertEqual(self._state_updates_to(attacker), [])

    async def test_state_update_from_non_hosting_worker_dropped(self) -> None:
        owner = ClientID.generate_client_id()
        hosting_worker = WorkerID.generate_worker_id("host")
        other_worker = WorkerID.generate_worker_id("other")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, hosting_worker, actor_id)

        update = ActorStateUpdate(actorId=actor_id, source=owner, workerId=other_worker, state=ActorState.alive)
        await self.controller.on_actor_state_update(other_worker, update)

        self.assertEqual(self._state_updates_to(owner), [])

    async def test_actor_message_routed_to_authoritative_owner(self) -> None:
        owner = ClientID.generate_client_id()
        attacker = ClientID.generate_client_id()
        worker = WorkerID.generate_worker_id("w")
        actor_id = ActorID.generate_actor_id()

        await self._place_actor(owner, worker, actor_id)

        # worker -> client: forged source must not redirect the message away from the owner
        message = ActorMessage(actorId=actor_id, source=attacker, payload=b"data")
        await self.controller.on_actor_message(bytes(worker), message)

        self.assertEqual(len(self._sent_to(owner)), 1)
        self.assertEqual(self._sent_to(attacker), [])


if __name__ == "__main__":
    unittest.main()
