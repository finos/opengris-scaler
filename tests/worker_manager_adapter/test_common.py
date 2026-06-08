import os
import tempfile
import unittest
from unittest.mock import MagicMock

from scaler.worker_manager_adapter.common import extract_desired_count, load_requirements_content


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [MagicMock(key=k, value=v) for k, v in capabilities.items()]
    return request


class TestLoadRequirementsContent(unittest.TestCase):
    def test_returns_literal_string(self):
        content = load_requirements_content("opengris-scaler>=1.0\nboto3\n")
        self.assertEqual(content, "opengris-scaler>=1.0\nboto3\n")

    def test_reads_valid_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("opengris-scaler\n")
            path = f.name
        try:
            content = load_requirements_content(path)
            self.assertEqual(content, "opengris-scaler\n")
        finally:
            os.unlink(path)

    def test_returns_literal_string_when_path_does_not_exist(self):
        content = load_requirements_content("/nonexistent/requirements.txt")
        self.assertEqual(content, "/nonexistent/requirements.txt")


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
