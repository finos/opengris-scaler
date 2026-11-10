import unittest

from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import ClientHeartbeat, Resource

# Resource.cpu is percent times ten: a 128-core host with every core busy reports 12800%.
FULLY_BUSY_128_CORE_CPU = 128_000


class TestResource(unittest.TestCase):
    def test_a_heartbeat_from_a_fully_busy_128_core_host_round_trips(self) -> None:
        """A process using more than 6553.5% CPU still sends its heartbeat."""
        heartbeat = ClientHeartbeat(
            resource=Resource(cpu=FULLY_BUSY_128_CORE_CPU, rss=1), latencyMicroseconds=0, hostname="host"
        )

        received = deserialize(serialize(heartbeat))

        assert isinstance(received, ClientHeartbeat)
        self.assertEqual(received.resource.cpu, FULLY_BUSY_128_CORE_CPU)
