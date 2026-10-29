import unittest

from scaler.protocol.capnp import TaskCapability, WorkerManagerCommand
from scaler.worker_manager.desired_concurrency import extract_desired_count


def _make_request(task_concurrency: int, capabilities: dict) -> WorkerManagerCommand.DesiredTaskConcurrencyRequest:
    # Build the real protocol struct (matching build_set_desired_command) and round-trip it through the
    # wire, so the test reads capabilities exactly as a worker manager does. A MagicMock here can invent
    # whatever attribute the code reads and hide a field-name mismatch (TaskCapability uses `name`, not
    # `key`), which is how a broken extract_desired_count once passed its tests while failing in production.
    command = WorkerManagerCommand(
        setDesiredTaskConcurrencyRequests=[
            WorkerManagerCommand.DesiredTaskConcurrencyRequest(
                taskConcurrency=task_concurrency,
                capabilities=[TaskCapability(name=name, value=value) for name, value in capabilities.items()],
            )
        ]
    )
    round_tripped = WorkerManagerCommand.from_bytes(command.to_bytes())
    return list(round_tripped.setDesiredTaskConcurrencyRequests)[0]


class TestExtractDesiredCount(unittest.TestCase):
    def test_returns_zero_for_empty_requests(self) -> None:
        self.assertEqual(extract_desired_count([], {"cpu": 4}), 0)

    def test_exact_capability_match(self) -> None:
        request = _make_request(task_concurrency=8, capabilities={"cpu": 4})
        self.assertEqual(extract_desired_count([request], {"cpu": 4}), 8)

    def test_subset_capability_match(self) -> None:
        request = _make_request(task_concurrency=3, capabilities={"cpu": 4})
        self.assertEqual(extract_desired_count([request], {"cpu": 4, "mem": 32}), 3)

    def test_empty_capabilities_matches_as_wildcard(self) -> None:
        request = _make_request(task_concurrency=5, capabilities={})
        self.assertEqual(extract_desired_count([request], {"cpu": 4}), 5)

    def test_request_with_more_capabilities_than_own_does_not_match(self) -> None:
        request = _make_request(task_concurrency=4, capabilities={"cpu": 4, "gpu": 1})
        self.assertEqual(extract_desired_count([request], {"cpu": 4}), 0)

    def test_returns_zero_when_no_request_matches(self) -> None:
        request = _make_request(task_concurrency=3, capabilities={"gpu": 1})
        self.assertEqual(extract_desired_count([request], {"cpu": 4}), 0)

    def test_sums_all_matching_requests(self) -> None:
        wildcard = _make_request(task_concurrency=2, capabilities={})
        specific = _make_request(task_concurrency=6, capabilities={"cpu": 4})
        self.assertEqual(extract_desired_count([wildcard, specific], {"cpu": 4}), 8)

    def test_sums_multiple_matches_excluding_non_matching(self) -> None:
        wildcard = _make_request(task_concurrency=4, capabilities={})
        matching = _make_request(task_concurrency=3, capabilities={"cpu": 4})
        non_matching = _make_request(task_concurrency=10, capabilities={"gpu": 1})
        self.assertEqual(
            extract_desired_count([wildcard, matching, non_matching], {"cpu": 4}), 7  # 4 + 3; non_matching excluded
        )

    def test_unlimited_capability_matches_any_request_value(self) -> None:
        """A provisioner with -1 (unlimited) for a capability matches any request value for it."""
        request = _make_request(task_concurrency=5, capabilities={"gpu": 8})
        self.assertEqual(extract_desired_count([request], {"gpu": -1}), 5)

    def test_unlimited_capability_matches_zero_request_value(self) -> None:
        """A provisioner with -1 (unlimited) matches a request with value 0."""
        request = _make_request(task_concurrency=3, capabilities={"gpu": 0})
        self.assertEqual(extract_desired_count([request], {"gpu": -1}), 3)

    def test_own_value_greater_than_request_matches(self) -> None:
        """A provisioner with more of a capability than requested matches (>= comparison)."""
        request = _make_request(task_concurrency=4, capabilities={"gpu": 2})
        self.assertEqual(extract_desired_count([request], {"gpu": 4}), 4)

    def test_own_value_less_than_request_does_not_match(self) -> None:
        """A provisioner with less of a capability than requested does not match."""
        request = _make_request(task_concurrency=4, capabilities={"gpu": 4})
        self.assertEqual(extract_desired_count([request], {"gpu": 2}), 0)

    def test_mixed_capabilities_with_unlimited_and_ge(self) -> None:
        """Multiple capabilities: -1 matches any value, others use >= comparison."""
        request = _make_request(task_concurrency=6, capabilities={"gpu": 2, "mem": 16})
        # gpu: -1 (unlimited) matches request 2; mem: 32 >= 16 matches
        self.assertEqual(extract_desired_count([request], {"gpu": -1, "mem": 32}), 6)

    def test_mixed_capabilities_one_fails_no_match(self) -> None:
        """If any capability fails the >= check (and is not -1), the whole request does not match."""
        request = _make_request(task_concurrency=6, capabilities={"gpu": 2, "mem": 64})
        # gpu: -1 matches; mem: 32 < 64 fails
        self.assertEqual(extract_desired_count([request], {"gpu": -1, "mem": 32}), 0)
