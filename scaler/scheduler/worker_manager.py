import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    DisconnectType,
    StateBalanceAdvice,
    StateWorker,
    Task,
    TaskCancel,
    TaskCancelFlags,
    TaskResult,
    TaskStatus,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.status import ProcessorStatus, Resource, WorkerManagerStatus, WorkerStatus
from scaler.scheduler.allocators.queued import QueuedAllocator
from scaler.scheduler.mixins import TaskManager, WorkerManager
from scaler.utility.mixins import Looper, Reporter


class VanillaWorkerManager(WorkerManager, Looper, Reporter):
    def __init__(
        self,
        per_worker_queue_size: int,
        timeout_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
    ):
        self._timeout_seconds = timeout_seconds
        self._load_balance_seconds = load_balance_seconds
        self._load_balance_trigger_times = load_balance_trigger_times

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since: Dict[bytes, Tuple[float, WorkerHeartbeat]] = dict()
        self._allocator = QueuedAllocator(per_worker_queue_size)

        self._last_balance_advice = None
        self._load_balance_advice_same_count = 0

    def register(self, binder: AsyncBinder, binder_monitor: AsyncConnector, task_manager: TaskManager):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._task_manager = task_manager

    async def assign_task_to_worker(self, task: Task) -> bool:
        worker = await self._allocator.assign_task(task.task_id)
        if worker is None:
            return False

        # send to worker
        await self._binder.send(worker, task)
        return True

    async def on_task_cancel(self, task_cancel: TaskCancel):
        worker = self._allocator.remove_task(task_cancel.task_id)
        if worker is None:
            logging.error(f"cannot find task_id={task_cancel.task_id.hex()} in task workers")
            return

        await self._binder.send(worker, TaskCancel(task_cancel.task_id))

    async def on_task_result(self, task_result: TaskResult):
        worker = self._allocator.remove_task(task_result.task_id)

        if task_result.status in {TaskStatus.Canceled, TaskStatus.NotFound}:
            if worker is not None:
                # The worker canceled the task, but the scheduler still had it queued. Re-route the task to another
                # worker.
                await self.__reroute_tasks([task_result.task_id])
            else:
                await self._task_manager.on_task_done(task_result)

            return

        if worker is None:
            logging.error(
                f"received unknown task result for task_id={task_result.task_id.hex()}, status={task_result.status} "
                f"might due to worker get disconnected or canceled"
            )
            return

        await self._task_manager.on_task_done(task_result)

    async def on_heartbeat(self, worker: bytes, info: WorkerHeartbeat):
        if await self._allocator.add_worker(worker):
            logging.info(f"worker {worker} connected")
            await self._binder_monitor.send(StateWorker(worker, b"connected"))

        self._worker_alive_since[worker] = (time.time(), info)
        await self._binder.send(worker, WorkerHeartbeatEcho())

    async def on_client_shutdown(self, client: bytes):
        for worker in self._allocator.get_worker_ids():
            await self.__shutdown_worker(worker)

    async def on_disconnect(self, source: bytes, request: DisconnectRequest):
        await self.__disconnect_worker(request.worker)
        await self._binder.send(source, DisconnectResponse(request.worker))

    async def routine(self):
        await self.__balance_request()
        await self.__clean_workers()

    def get_status(self) -> WorkerManagerStatus:
        worker_to_task_numbers = self._allocator.statistics()
        return WorkerManagerStatus(
            [
                self.__worker_status_from_heartbeat(worker, worker_to_task_numbers[worker], last, info)
                for worker, (last, info) in self._worker_alive_since.items()
            ]
        )

    def __worker_status_from_heartbeat(
        self, worker: bytes, worker_task_numbers: Dict, last: float, info: WorkerHeartbeat
    ) -> WorkerStatus:
        current_processor = next((p for p in info.processors if not p.suspended), None)
        n_suspended = sum(1 for p in info.processors if p.suspended)

        if current_processor:
            ITL = f"{int(current_processor.initialized)}{int(current_processor.has_task)}{int(info.task_lock)}"
        else:
            ITL = f"00{int(info.task_lock)}"

        processor_statuses = [
            ProcessorStatus(
                p.pid,
                p.initialized,
                p.has_task,
                p.suspended,
                Resource(p.cpu, p.rss, info.rss_free),
            )
            for p in info.processors
        ]

        return WorkerStatus(
            worker_id=worker,
            agent=Resource(info.agent_cpu, info.agent_rss, info.rss_free),
            total_processors=Resource(
                sum(p.cpu for p in info.processors), sum(p.rss for p in info.processors), info.rss_free
            ),
            free=worker_task_numbers["free"],
            sent=worker_task_numbers["sent"],
            queued=info.queued_tasks,
            suspended=n_suspended,
            lag_us=info.latency_us,
            last_s=int(time.time() - last),
            ITL=ITL,
            processor_statuses=processor_statuses,
        )

    def has_available_worker(self) -> bool:
        return self._allocator.has_available_worker()

    def get_worker_by_task_id(self, task_id: bytes) -> bytes:
        return self._allocator.get_worker_by_task_id(task_id)

    def get_worker_ids(self) -> Set[bytes]:
        return self._allocator.get_worker_ids()

    async def __balance_request(self):
        if self._load_balance_seconds <= 0:
            return

        current_advice = self._allocator.balance()
        if self._last_balance_advice == current_advice:
            self._load_balance_advice_same_count += 1
        else:
            self._last_balance_advice = current_advice
            self._load_balance_advice_same_count = 0

        if 0 < self._load_balance_advice_same_count < self._load_balance_trigger_times:
            return

        await self.__do_balance(current_advice)

    async def __do_balance(self, current_advice: Dict[bytes, List[bytes]]):
        if not current_advice:
            return

        logging.info(f"balance: {current_advice}")
        for worker, tasks in current_advice.items():
            await self._binder_monitor.send(StateBalanceAdvice(worker, tasks))

        task_cancel_flags = TaskCancelFlags(force=True)

        self._last_balance_advice = current_advice
        for worker, tasks in current_advice.items():
            for task in tasks:
                await self._binder.send(worker, TaskCancel(task, flags=task_cancel_flags))

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            dead_worker
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > self._timeout_seconds
        ]
        for dead_worker in dead_workers:
            await self.__disconnect_worker(dead_worker)

    async def __reroute_tasks(self, task_ids: List[bytes]):
        for task_id in task_ids:
            await self._task_manager.on_task_reroute(task_id)

    async def __disconnect_worker(self, worker: bytes):
        """return True if disconnect worker success"""
        if worker not in self._worker_alive_since:
            return

        logging.info(f"worker {worker} disconnected")
        await self._binder_monitor.send(StateWorker(worker, b"disconnected"))
        self._worker_alive_since.pop(worker)

        task_ids = self._allocator.remove_worker(worker)
        if not task_ids:
            return

        logging.info(f"rerouting {len(task_ids)} tasks")
        await self.__reroute_tasks(task_ids)

    async def __shutdown_worker(self, worker: bytes):
        await self._binder.send(worker, ClientDisconnect(DisconnectType.Shutdown))
        await self.__disconnect_worker(worker)
