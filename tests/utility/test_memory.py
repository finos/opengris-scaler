import os
import tempfile
import unittest
from unittest import mock

import psutil

from scaler.utility import memory
from scaler.utility.memory import get_memory_limit_and_available, get_process_memory


class TestGetProcessMemory(unittest.TestCase):
    def test_uses_pss_when_available(self) -> None:
        process = mock.Mock()
        process.memory_full_info.return_value = mock.Mock(pss=1234, rss=9999)
        self.assertEqual(get_process_memory(process), 1234)

    def test_falls_back_to_rss_where_pss_unavailable(self) -> None:
        # macOS/Windows: memory_full_info() has rss but no pss field.
        process = mock.Mock()
        process.memory_full_info.return_value = mock.Mock(spec=["rss"], rss=5678)
        self.assertEqual(get_process_memory(process), 5678)

    def test_propagates_error_when_process_gone(self) -> None:
        # A vanished/zombie process raises; callers handle that explicitly rather than a silent fallback.
        process = mock.Mock()
        process.memory_full_info.side_effect = psutil.NoSuchProcess(1)
        with self.assertRaises(psutil.NoSuchProcess):
            get_process_memory(process)


class TestGetMemoryLimitAndAvailable(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def _path(self, name: str, content: str) -> str:
        path = os.path.join(self._tmp.name, name)
        with open(path, "w") as handle:
            handle.write(content)
        return path

    def _patch_cgroup_paths(self, v2_max=None, v2_current=None, v1_limit=None, v1_usage=None):
        missing = os.path.join(self._tmp.name, "does_not_exist")
        return mock.patch.multiple(
            memory,
            _CGROUP_V2_MAX=self._path("v2_max", v2_max) if v2_max is not None else missing,
            _CGROUP_V2_CURRENT=self._path("v2_current", v2_current) if v2_current is not None else missing,
            _CGROUP_V1_LIMIT=self._path("v1_limit", v1_limit) if v1_limit is not None else missing,
            _CGROUP_V1_USAGE=self._path("v1_usage", v1_usage) if v1_usage is not None else missing,
        )

    def _patch_host(self, total: int, available: int):
        return mock.patch.object(
            memory.psutil, "virtual_memory", return_value=mock.Mock(total=total, available=available)
        )

    def test_cgroup_v2(self) -> None:
        with self._patch_cgroup_paths(v2_max="2000", v2_current="500"):
            self.assertEqual(get_memory_limit_and_available(), (2000, 1500))

    def test_cgroup_v2_unlimited_falls_back_to_host(self) -> None:
        with self._patch_cgroup_paths(v2_max="max"), self._patch_host(100, 40):
            self.assertEqual(get_memory_limit_and_available(), (100, 40))

    def test_cgroup_v1(self) -> None:
        with self._patch_cgroup_paths(v1_limit="3000", v1_usage="1200"):
            self.assertEqual(get_memory_limit_and_available(), (3000, 1800))

    def test_cgroup_v1_unlimited_sentinel_falls_back_to_host(self) -> None:
        with self._patch_cgroup_paths(v1_limit=str(2**63 - 1), v1_usage="10"), self._patch_host(100, 40):
            self.assertEqual(get_memory_limit_and_available(), (100, 40))

    def test_no_cgroup_falls_back_to_host(self) -> None:
        with self._patch_cgroup_paths(), self._patch_host(100, 40):
            self.assertEqual(get_memory_limit_and_available(), (100, 40))

    def test_usage_above_limit_clamps_available_to_zero(self) -> None:
        with self._patch_cgroup_paths(v2_max="1000", v2_current="1500"):
            self.assertEqual(get_memory_limit_and_available(), (1000, 0))
