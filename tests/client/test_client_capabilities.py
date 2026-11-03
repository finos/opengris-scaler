"""Which capabilities the tasks of a client carry: the client's, a submission's own, or the parent task's."""

import unittest
from typing import Dict, Optional

from scaler import Client, SchedulerClusterCombo
from scaler.config.section.scheduler import PolicyConfig
from scaler.protocol.helpers import capabilities_to_dict
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.processor.processor import Processor
from tests.utility.utility import logging_test_name

GPU = {"gpu": 1}


def task_capabilities(_: int = 0) -> Dict[str, int]:
    """The capabilities of the running task, as its worker received them."""
    current_processor = Processor.get_current_processor()
    assert current_processor is not None

    current_task = current_processor.current_task()
    assert current_task is not None

    return capabilities_to_dict(current_task.capabilities)


def nested_task_capabilities(capabilities: Optional[Dict[str, int]]) -> Dict[str, int]:
    with Client(capabilities=capabilities) as client:
        return client.submit(task_capabilities).result()


def serialized_client_task_capabilities(client: Client) -> Dict[str, int]:
    return client.submit(task_capabilities).result()


class TestClientCapabilities(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(
            n_workers=3,
            per_worker_capabilities={"gpu": -1},
            event_loop="builtin",
            scaler_policy=PolicyConfig(policy_content="allocate=capability; scaling=static"),
        )
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_every_submission_takes_the_client_capabilities(self) -> None:
        with Client(self.address, capabilities=GPU) as client:
            self.assertEqual(client.submit(task_capabilities).result(), GPU)
            self.assertEqual(client.map(task_capabilities, [1, 2]), [GPU, GPU])
            self.assertEqual(client.get({"a": 1, "b": (task_capabilities, "a")}, keys=["b"]), {"b": GPU})

    def test_a_submission_capabilities_replace_the_client_ones(self) -> None:
        with Client(self.address, capabilities=GPU) as client:
            future = client.submit_verbose(task_capabilities, args=(), kwargs={}, capabilities={})
            self.assertEqual(future.result(), {})

    def test_a_client_outside_a_worker_defaults_to_no_capabilities(self) -> None:
        with Client(self.address) as client:
            self.assertEqual(client.submit(task_capabilities).result(), {})

    def test_a_serialized_client_keeps_its_capabilities(self) -> None:
        """The parent task carries none, so the nested task's capabilities can only come from the client."""
        with Client(self.address, capabilities=GPU) as client:
            future = client.submit_verbose(
                serialized_client_task_capabilities, args=(client,), kwargs={}, capabilities={}
            )
            self.assertEqual(future.result(), GPU)

    def test_a_client_inside_a_worker_takes_its_task_capabilities(self) -> None:
        with Client(self.address) as client:
            future = client.submit_verbose(nested_task_capabilities, args=(None,), kwargs={}, capabilities=GPU)
            self.assertEqual(future.result(), GPU)

    def test_a_client_inside_a_worker_given_no_capabilities_drops_its_task_ones(self) -> None:
        with Client(self.address) as client:
            future = client.submit_verbose(nested_task_capabilities, args=({},), kwargs={}, capabilities=GPU)
            self.assertEqual(future.result(), {})


if __name__ == "__main__":
    unittest.main()
