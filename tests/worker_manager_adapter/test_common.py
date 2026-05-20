import unittest
from unittest.mock import MagicMock

from scaler.worker_manager_adapter.common import extract_desired_count


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [MagicMock(key=k, value=v) for k, v in capabilities.items()]
    return request


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
