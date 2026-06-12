import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.protocol.capnp import ActorState, ActorStateUpdate
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

_WAIT_TIMEOUT_SECONDS = 60
_PLACEMENT_RETRY_SECONDS = 0.5


class EchoActor:
    def __init__(self, value: int):
        self.value = value


class FailingActor:
    def __init__(self):
        raise ValueError("constructor boom")


def wait_for_state(handle, expected_state: ActorState, timeout_seconds: float = _WAIT_TIMEOUT_SECONDS):
    reached = handle.wait_for_state(expected_state, timeout=timeout_seconds)
    if reached != expected_state:
        raise AssertionError(
            f"actor reached {reached.name} while waiting for {expected_state.name}: " f"{handle.death_reason}"
        )
    return reached


def create_actor_when_pool_ready(client, cls, *args, deadline_seconds: float = _WAIT_TIMEOUT_SECONDS):
    """Creates an actor, retrying on placementFailed.

    A worker released from a designation rejoins the pool with its next heartbeat, so a create
    issued right after a destroy can race the pool re-registration; placement is fail-fast by
    design, hence the retry loop.
    """
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        handle = client.create_actor(cls, *args)

        remaining = max(deadline - time.monotonic(), 0.1)
        reached = handle.wait_for_state(ActorState.alive, timeout=remaining)
        if reached == ActorState.alive:
            return handle

        if handle.death_reason != ActorStateUpdate.DeathInfo.Reason.placementFailed:
            raise AssertionError(f"actor died while being placed: {handle.death_reason}")

        time.sleep(_PLACEMENT_RETRY_SECONDS)

    raise AssertionError(f"could not place an actor within {deadline_seconds}s")


class TestActor(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(n_workers=2, event_loop="builtin")
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_create_and_destroy_actor(self):
        with Client(address=self.address) as client:
            actor = client.create_actor(EchoActor, 42)

            # creation is asynchronous: the handle is usable immediately
            self.assertFalse(actor.dead)

            wait_for_state(actor, ActorState.alive)
            self.assertFalse(actor.dead)
            self.assertIsNone(actor.death_reason)

            actor.destroy()
            wait_for_state(actor, ActorState.dead)
            self.assertTrue(actor.dead)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.destroyed)

            # destroying a dead actor is a no-op
            actor.destroy()
            self.assertTrue(actor.dead)

    def test_create_actor_force_destroy(self):
        with Client(address=self.address) as client:
            actor = client.create_actor(EchoActor, value=1)
            wait_for_state(actor, ActorState.alive)

            actor.destroy(force=True)
            wait_for_state(actor, ActorState.dead)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.destroyed)

    def test_actor_designation_cycle(self):
        # one actor occupies one whole worker: with 2 workers, 2 actors exhaust the pool, and
        # destroying one returns its worker for a new designation
        with Client(address=self.address) as client:
            first = client.create_actor(EchoActor, 1)
            second = client.create_actor(EchoActor, 2)
            wait_for_state(first, ActorState.alive)
            wait_for_state(second, ActorState.alive)

            third = client.create_actor(EchoActor, 3)
            wait_for_state(third, ActorState.dead)
            self.assertEqual(third.death_reason, ActorStateUpdate.DeathInfo.Reason.placementFailed)

            first.destroy()
            wait_for_state(first, ActorState.dead)

            fourth = create_actor_when_pool_ready(client, EchoActor, 4)
            self.assertFalse(fourth.dead)

            second.destroy()
            fourth.destroy()
            wait_for_state(second, ActorState.dead)
            wait_for_state(fourth, ActorState.dead)

    def test_constructor_failure(self):
        with Client(address=self.address) as client:
            actor = client.create_actor(FailingActor)

            wait_for_state(actor, ActorState.dead)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.constructorFailed)

    def test_create_actor_rejects_non_class(self):
        with Client(address=self.address) as client:
            with self.assertRaises(TypeError):
                client.create_actor(lambda: None)


class TestActorNoWorkers(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(n_workers=0, event_loop="builtin")
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_create_actor_without_workers(self):
        with Client(address=self.address) as client:
            actor = client.create_actor(EchoActor, 1)

            wait_for_state(actor, ActorState.dead, timeout_seconds=30)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.placementFailed)

            # the scheduler must stay healthy after the failed placement: a second create is
            # still answered
            second_actor = client.create_actor(EchoActor, 2)
            wait_for_state(second_actor, ActorState.dead, timeout_seconds=30)
            self.assertEqual(second_actor.death_reason, ActorStateUpdate.DeathInfo.Reason.placementFailed)
