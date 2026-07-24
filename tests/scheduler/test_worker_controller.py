import asyncio
import time
import unittest
from unittest.mock import MagicMock

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.vanilla_policy_controller import VanillaPolicyController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _DeadableBinder:
    """An async binder whose send() fails as a departed peer for any worker marked dead."""

    def __init__(self):
        self.dead = set()

    async def send(self, to, message):
        if WorkerID(bytes(to)) in self.dead:
            raise ConnectorSocketClosedByRemoteEndError(
                ErrorCode.ConnectorSocketClosedByRemoteEnd, "worker socket closed by remote end"
            )


class _NullMonitor:
    async def send(self, message):
        return None


class TestWorkerControllerMassEviction(unittest.TestCase):
    """A whole batch of workers dropping at once must be cleaned up without crashing the scheduler.

    With one shared capability every task fits every worker, so a task shed from a dead worker is
    reassigned to another dead-but-still-registered worker whose send also fails. __send_to_worker swallows
    that departed-peer error instead of re-entering the disconnect path, so the heartbeat sweep disconnects
    the whole batch iteratively, without a per-worker reroute cascade or unbounded coroutine recursion.
    """

    N_WORKERS = 300  # a large simultaneous batch, well past any reasonable recursion limit

    @staticmethod
    def _make_task(index: int) -> Task:
        return Task(
            taskId=TaskID(f"task-{index}".encode()),
            source=ClientID(b"client"),
            metadata=b"",
            funcObjectId=b"",
            functionArgs=[],
            capabilities={},
        )

    def test_mass_eviction_is_handled_without_crashing(self):
        config = MagicMock()
        config.get_config.side_effect = lambda key: 0 if key == "worker_timeout_seconds" else MagicMock()
        policy = VanillaPolicyController("simple", "allocate=capability; scaling=vanilla")
        worker_controller = VanillaWorkerController(config, policy)
        task_controller = VanillaTaskController(config)

        binder = _DeadableBinder()
        monitor = _NullMonitor()
        client_controller = MagicMock()
        client_controller.on_task_finish.return_value = None
        object_controller = MagicMock()
        object_controller.get_object_name.return_value = b""
        graph_controller = MagicMock()
        graph_controller.is_graph_subtask.return_value = False

        worker_controller.register(binder, monitor, task_controller)  # type: ignore[arg-type]
        task_controller.register(
            binder,  # type: ignore[arg-type]
            monitor,  # type: ignore[arg-type]
            client_controller,
            object_controller,
            worker_controller,
            graph_controller,
        )

        # Register N workers with a stale last-heartbeat so the sweep times all of them out at once
        # (bypassing on_heartbeat; replicate the state it maintains).
        manager_id = b"pod-manager"
        for i in range(self.N_WORKERS):
            worker_id = WorkerID(f"worker-{i}".encode())
            policy.add_worker(worker_id, {"capA": -1}, 10)
            worker_controller._worker_alive_since[worker_id] = (time.time() - 3600, None)
            worker_controller._worker_to_manager[worker_id] = manager_id
            worker_controller._manager_to_workers.setdefault(manager_id, set()).add(worker_id)

        async def scenario():
            for i in range(self.N_WORKERS):  # one running task per worker, all sent while live
                await task_controller.on_task_new(self._make_task(i))
            for i in range(self.N_WORKERS):  # a batch of pods is evicted at once
                binder.dead.add(WorkerID(f"worker-{i}".encode()))
            # Must not raise RecursionError: the sweep disconnects the batch iteratively and each failed
            # reroute send is swallowed rather than re-entering the disconnect path.
            await worker_controller.routine()

        _run(scenario())

        # Every dead worker was disconnected; none is left registered.
        self.assertEqual(len(policy.get_worker_ids()), 0)


if __name__ == "__main__":
    unittest.main()
