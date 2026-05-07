"""End-to-end delivery test for ``SyncSubscriber``

Wires a ``AsyncPublisher`` to a ``SyncSubscriber`` through the default ``NetworkBackend``
and asserts that a published message is delivered to the subscriber's callback. The
publisher does not prepend a topic prefix, so the subscriber's topic filter must accept
unprefixed payloads.
"""

import asyncio
import threading
import time
import unittest
from datetime import timedelta

from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import BaseMessage, StateBalanceAdvice
from scaler.utility.identifiers import WorkerID

# The publisher binds to an ephemeral loopback port.
_BIND_ADDRESS = AddressConfig.from_string("tcp://127.0.0.1:0")
# Time budget for the message to be delivered after publish.
_RECEIVE_TIMEOUT_SECONDS = 5.0
# Pub/Sub is best-effort — messages published before the SUB socket has finished
# subscribing are dropped — so warm up briefly between subscriber start and publish.
_SUBSCRIPTION_WARMUP_SECONDS = 0.3
# Per-recv timeout so we can interrupt the polling loop without tearing down the
# context mid-recv (which crashes libzmq).
_POLL_TIMEOUT = timedelta(seconds=1)


class TestSyncSubscriberReceivesPublishedMessages(unittest.TestCase):
    def test_subscriber_receives_published_message(self):
        backend = get_network_backend_from_env(io_threads=1)
        loop = asyncio.new_event_loop()
        publisher = backend.create_async_publisher(identity=b"test-publisher")
        loop.run_until_complete(publisher.bind(_BIND_ADDRESS))

        received: list[BaseMessage] = []
        received_event = threading.Event()

        def on_message(message: BaseMessage) -> None:
            received.append(message)
            received_event.set()

        assert publisher.address is not None

        subscriber = backend.create_sync_subscriber(
            identity=generate_identity_from_name("test-subscriber"),
            address=publisher.address,
            timeout=_POLL_TIMEOUT,
            callback=on_message,
        )

        def poll_loop() -> None:
            while True:
                try:
                    subscriber.run()
                    break
                except TimeoutError:
                    continue

        poller = threading.Thread(target=poll_loop, daemon=True)
        poller.start()
        try:
            time.sleep(_SUBSCRIPTION_WARMUP_SECONDS)
            loop.run_until_complete(publisher.send(StateBalanceAdvice(workerId=WorkerID(b"worker-a"), taskIds=[])))

            self.assertTrue(
                received_event.wait(timeout=_RECEIVE_TIMEOUT_SECONDS),
                "subscriber did not receive the published message within the timeout",
            )
            self.assertEqual(len(received), 1)
            self.assertIsInstance(received[0], StateBalanceAdvice)
        finally:
            subscriber.destroy()
            poller.join(timeout=2.0)
            publisher.destroy()
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()
            backend.destroy()


if __name__ == "__main__":
    unittest.main()
