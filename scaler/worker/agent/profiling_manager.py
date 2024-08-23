import dataclasses
import time
from typing import Dict, Optional

import psutil

from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import ProfilingManager


@dataclasses.dataclass
class _ProcessProfiler:
    process: psutil.Process

    current_task_id: Optional[bytes] = None

    start_time: Optional[float] = None
    start_cpu_time: Optional[float] = None
    init_memory_rss: Optional[int] = None
    peak_memory_rss: Optional[int] = None


class VanillaProfilingManager(ProfilingManager, Looper):
    def __init__(self):
        self._process_profiler_by_pid: Dict[int, _ProcessProfiler] = {}

    def on_process_start(self, pid: int):
        if pid in self._process_profiler_by_pid:
            raise ValueError(f"process {pid=} is already registered.")

        self._process_profiler_by_pid[pid] = _ProcessProfiler(psutil.Process(pid))

    def on_process_end(self, pid: int):
        if pid not in self._process_profiler_by_pid:
            raise ValueError(f"process {pid=} is not registered.")

        self._process_profiler_by_pid.pop(pid)

    def on_task_start(self, pid: int, task_id: bytes):
        process_profiler = self._process_profiler_by_pid.get(pid)

        if process_profiler is None:
            raise ValueError(f"process {pid=} is not registered.")

        process_profiler.current_task_id = task_id

        process = process_profiler.process

        process_profiler.start_time = self.__process_time()
        process_profiler.start_cpu_time = self.__process_cpu_time(process)
        process_profiler.init_memory_rss = self.__process_memory_rss(process)
        process_profiler.peak_memory_rss = process_profiler.init_memory_rss

    def on_task_end(self, pid: int, task_id: bytes) -> ProfileResult:
        process_profiler = self._process_profiler_by_pid.get(pid)

        if process_profiler is None:
            raise ValueError(f"process {pid=} is not registered.")

        if task_id != process_profiler.current_task_id:
            raise ValueError(f"task {task_id=!r} is not the current task task_id={process_profiler.current_task_id!r}.")

        assert process_profiler.start_time is not None
        assert process_profiler.init_memory_rss is not None
        assert process_profiler.peak_memory_rss is not None

        process = process_profiler.process

        time_delta = self.__process_time() - process_profiler.start_time
        cpu_time_delta = self.__process_cpu_time(process) - process_profiler.start_cpu_time
        memory_delta = process_profiler.peak_memory_rss - process_profiler.init_memory_rss

        process_profiler.current_task_id = None
        process_profiler.init_memory_rss = None
        process_profiler.peak_memory_rss = None

        return ProfileResult(time_delta, memory_delta, cpu_time_delta)

    async def routine(self):
        for process_profiler in self._process_profiler_by_pid.values():
            if process_profiler.current_task_id is not None:
                process_profiler.peak_memory_rss = max(
                    process_profiler.peak_memory_rss, self.__process_memory_rss(process_profiler.process)
                )

    @staticmethod
    def __process_time():
        return time.monotonic()

    @staticmethod
    def __process_cpu_time(process: psutil.Process) -> float:
        cpu_times = process.cpu_times()
        return cpu_times.user + cpu_times.system

    @staticmethod
    def __process_memory_rss(process: psutil.Process) -> int:
        return process.memory_info().rss
