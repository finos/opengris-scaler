import os
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.protocol.capnp import ActorState, ActorStateUpdate
from scaler.utility.exceptions import ActorDeadError
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


class GreeterActor:
    def __receive__(self, payload: bytes):
        if payload == b"hello actor":
            return b"hello client"
        return None


class WedgingActor:
    """Its constructor never returns, so the actor reaches `creating` but never `alive`.

    Used to exercise force-kill (`destroy(force=True)`), whose whole purpose is tearing down a
    process the actor itself cannot cooperate with.
    """

    def __init__(self):
        while True:
            time.sleep(3600)


class CrashActor:
    """Crashes its hosting processor on the first message, simulating an unexpected exit."""

    def __receive__(self, payload: bytes):
        os._exit(1)


class GarbageReplyActor:
    """Returns a non-bytes-like value, which must be rejected, not coerced to a junk payload."""

    def __receive__(self, payload: bytes):
        return 5


def echo_value(value: int) -> int:
    return value


def warm_up_pool(client, n_tasks: int = 1):
    """Runs tasks to completion so workers are registered and idle before any actor is created.

    A create issued against a freshly started cluster can otherwise race worker registration and
    fail fast with placementFailed; running a task first proves at least one worker is in the
    pool.
    """
    futures = [client.submit(echo_value, i) for i in range(n_tasks)]
    for future in futures:
        future.result(timeout=_WAIT_TIMEOUT_SECONDS)


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
            warm_up_pool(client)
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
            actor = create_actor_when_pool_ready(client, EchoActor, 1)

            actor.destroy(force=True)
            wait_for_state(actor, ActorState.dead)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.destroyed)

    def test_force_destroy_wedged_constructor(self):
        # force-kill's actual purpose: tearing down an actor that cannot cooperate. The wedged
        # constructor never returns, so the actor never reaches `alive`; kill must still destroy it
        with Client(address=self.address) as client:
            warm_up_pool(client)
            actor = client.create_actor(WedgingActor)

            wait_for_state(actor, ActorState.creating)
            self.assertFalse(actor.dead)

            actor.destroy(force=True)
            wait_for_state(actor, ActorState.dead)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.destroyed)

    def test_actor_crash_reports_actor_crashed(self):
        with Client(address=self.address) as client:
            actor = create_actor_when_pool_ready(client, CrashActor)

            # the actor exits its processor unexpectedly while handling the message
            actor.__send__(b"crash")

            wait_for_state(actor, ActorState.dead)
            self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.actorCrashed)

    def test_client_disconnect_destroys_actor(self):
        # an actor's lifetime is bound to its owning client: disconnecting kills it
        client = Client(address=self.address)
        try:
            actor = create_actor_when_pool_ready(client, EchoActor, 1)
            self.assertFalse(actor.dead)
        finally:
            client.disconnect()

        self.assertTrue(actor.dead)
        self.assertEqual(actor.death_reason, ActorStateUpdate.DeathInfo.Reason.clientDisconnected)

    def test_actor_designation_cycle(self):
        # one actor occupies one whole worker: with 2 workers, 2 actors exhaust the pool, and
        # destroying one returns its worker for a new designation
        with Client(address=self.address) as client:
            first = create_actor_when_pool_ready(client, EchoActor, 1)
            second = create_actor_when_pool_ready(client, EchoActor, 2)

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

    def test_mixed_actors_and_tasks(self):
        # an actor-designated worker leaves the task pool; normal clients keep the balancing
        # behavior on the remaining workers, and get the full pool back after the destroy
        with Client(address=self.address) as client:
            actor = create_actor_when_pool_ready(client, GreeterActor)

            results = [client.submit(echo_value, i).result(timeout=_WAIT_TIMEOUT_SECONDS) for i in range(8)]
            self.assertEqual(results, list(range(8)))

            actor.__send__(b"hello actor")
            self.assertEqual(actor.__receive__(timeout=_WAIT_TIMEOUT_SECONDS), b"hello client")

            actor.destroy()
            wait_for_state(actor, ActorState.dead)

            self.assertEqual(client.submit(echo_value, 42).result(timeout=_WAIT_TIMEOUT_SECONDS), 42)

    def test_actor_message_bidirectional(self):
        with Client(address=self.address) as client:
            actor = create_actor_when_pool_ready(client, GreeterActor)

            # client -> actor, then actor -> client
            actor.__send__(b"hello actor")
            self.assertEqual(actor.__receive__(timeout=_WAIT_TIMEOUT_SECONDS), b"hello client")

            # the exchange is repeatable on the same actor
            actor.__send__(b"hello actor")
            self.assertEqual(actor.__receive__(timeout=_WAIT_TIMEOUT_SECONDS), b"hello client")

            actor.destroy()
            wait_for_state(actor, ActorState.dead)

    def test_actor_message_receive_timeout_and_death(self):
        with Client(address=self.address) as client:
            actor = create_actor_when_pool_ready(client, GreeterActor)

            # a payload the actor does not reply to: __receive__ times out
            actor.__send__(b"no reply expected")
            with self.assertRaises(TimeoutError):
                actor.__receive__(timeout=1)

            # once the actor is dead, __receive__ raises instead of blocking forever
            actor.destroy()
            wait_for_state(actor, ActorState.dead)
            with self.assertRaises(ActorDeadError):
                actor.__receive__(timeout=_WAIT_TIMEOUT_SECONDS)

    def test_non_bytes_reply_is_dropped(self):
        with Client(address=self.address) as client:
            actor = create_actor_when_pool_ready(client, GarbageReplyActor)

            # the actor returns a non-bytes-like value: the processor must reject it rather than
            # coerce it (e.g. bytes(5) -> b"\x00\x00\x00\x00\x00") and deliver garbage
            actor.__send__(b"ping")
            with self.assertRaises(TimeoutError):
                actor.__receive__(timeout=2)

            actor.destroy()
            wait_for_state(actor, ActorState.dead)

    def test_constructor_failure(self):
        with Client(address=self.address) as client:
            warm_up_pool(client)
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
