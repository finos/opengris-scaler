import unittest

from scaler.protocol.capnp import ScalingManagerStatus, StateTask, StateWorker, TaskCapability, TaskState, WorkerState
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status


def _roundtrip_state_task(**kwargs) -> StateTask:
    """Build a StateTask and return a deserialized copy (simulates ZMQ path)."""
    return StateTask.from_bytes(StateTask(**kwargs).to_bytes())


def _roundtrip_state_worker(**kwargs) -> StateWorker:
    """Build a StateWorker and return a deserialized copy (simulates ZMQ path)."""
    return StateWorker.from_bytes(StateWorker(**kwargs).to_bytes())


class TestEnumFieldValueComparison(unittest.TestCase):
    """After Cap'n Proto round-trip, enum fields become EnumFieldValue objects.

    == works, but hash() differs from the real enum member so `in {set}` fails.
    The UI code must use `any(val == s for s in ...)` instead of `val in {set}`.
    """

    def test_deserialized_task_state_equality(self):
        """Deserialized TaskState should compare equal via == to the enum member."""
        for ts in (TaskState.success, TaskState.running, TaskState.canceled, TaskState.failed):
            st = _roundtrip_state_task(state=ts, taskId=b"t1", functionName=b"fn", worker=b"w1")
            self.assertEqual(st.state, ts)

    def test_deserialized_task_state_not_in_set(self):
        """Demonstrate that deserialized enum values fail hash-based set lookup."""
        completed = {TaskState.success, TaskState.canceled, TaskState.failed, TaskState.failedWorkerDied}
        st = _roundtrip_state_task(state=TaskState.success, taskId=b"t1", functionName=b"fn", worker=b"w1")
        # This is the bug that was fixed — `in` on a set uses hash, which doesn't match
        self.assertFalse(st.state in completed, "EnumFieldValue should NOT match via `in` on a set")

    def test_deserialized_task_state_any_pattern(self):
        """The any() pattern used in the fix should correctly identify completed states."""
        completed = (
            TaskState.success,
            TaskState.canceled,
            TaskState.canceledNotFound,
            TaskState.failed,
            TaskState.failedWorkerDied,
        )

        for ts in completed:
            st = _roundtrip_state_task(state=ts, taskId=b"t1", functionName=b"fn", worker=b"w1")
            self.assertTrue(any(st.state == s for s in completed), f"{ts} should match via any()")

        non_completed = (TaskState.running, TaskState.inactive, TaskState.canceling, TaskState.balanceCanceling)
        for ts in non_completed:
            st = _roundtrip_state_task(state=ts, taskId=b"t1", functionName=b"fn", worker=b"w1")
            self.assertFalse(any(st.state == s for s in completed), f"{ts} should NOT match completed")

    def test_deserialized_worker_state_equality(self):
        """Deserialized WorkerState should compare equal via ==."""
        for ws in (WorkerState.connected, WorkerState.disconnected):
            sw = _roundtrip_state_worker(state=ws, workerId=b"w1", capabilities=[])
            self.assertEqual(sw.state, ws)


class TestEnumAsStrCamelCase(unittest.TestCase):
    """_as_str() must return camelCase strings that match the JS frontend expectations."""

    def test_task_state_as_str(self):
        expected = {
            TaskState.success: "success",
            TaskState.running: "running",
            TaskState.inactive: "inactive",
            TaskState.canceling: "canceling",
            TaskState.balanceCanceling: "balanceCanceling",
            TaskState.canceled: "canceled",
            TaskState.canceledNotFound: "canceledNotFound",
            TaskState.failed: "failed",
            TaskState.failedWorkerDied: "failedWorkerDied",
        }
        for ts, expected_str in expected.items():
            st = _roundtrip_state_task(state=ts, taskId=b"t1", functionName=b"fn", worker=b"w1")
            self.assertEqual(st.state._as_str(), expected_str)

    def test_worker_state_as_str(self):
        expected = {WorkerState.connected: "connected", WorkerState.disconnected: "disconnected"}
        for ws, expected_str in expected.items():
            sw = _roundtrip_state_worker(state=ws, workerId=b"w1", capabilities=[])
            self.assertEqual(sw.state._as_str(), expected_str)


class TestCapabilitiesToDict(unittest.TestCase):
    """capabilities_to_dict must handle Cap'n Proto list-of-struct capabilities after round-trip."""

    def test_state_task_capabilities(self):
        caps = [TaskCapability(name="gpu", value=2), TaskCapability(name="cpu", value=8)]
        st = _roundtrip_state_task(
            state=TaskState.running, taskId=b"t1", functionName=b"fn", worker=b"w1", capabilities=caps
        )
        result = capabilities_to_dict(st.capabilities)
        self.assertEqual(result, {"gpu": 2, "cpu": 8})

    def test_state_worker_capabilities(self):
        caps = [TaskCapability(name="gpu", value=4)]
        sw = _roundtrip_state_worker(state=WorkerState.connected, workerId=b"w1", capabilities=caps)
        result = capabilities_to_dict(sw.capabilities)
        self.assertEqual(result, {"gpu": 4})

    def test_empty_capabilities(self):
        st = _roundtrip_state_task(
            state=TaskState.running, taskId=b"t1", functionName=b"fn", worker=b"w1", capabilities=[]
        )
        result = capabilities_to_dict(st.capabilities)
        self.assertEqual(result, {})


class TestScalingManagerStatusRoundTrip(unittest.TestCase):
    """build_scaling_manager_status must produce structs that survive serialization
    and expose data via attribute access (not dict access)."""

    def test_managed_workers_round_trip(self):
        managed = {b"mgr_a": [b"w1", b"w2"], b"mgr_b": [b"w3"]}
        sms = ScalingManagerStatus.from_bytes(build_scaling_manager_status(managed).to_bytes())

        result = {}
        for pair in sms.managedWorkers:
            result[bytes(pair.workerManagerID)] = [bytes(w) for w in pair.workerIDs]

        self.assertEqual(result, managed)

    def test_worker_manager_details_round_trip(self):
        details = [
            {
                "worker_manager_id": b"mgr_a",
                "identity": "host1",
                "last_seen_s": 5,
                "max_task_concurrency": 4,
                "capabilities": "gpu",
                "pending_workers": 2,
            }
        ]
        sms = ScalingManagerStatus.from_bytes(build_scaling_manager_status({b"mgr_a": [b"w1"]}, details).to_bytes())

        detail_list = list(sms.workerManagerDetails)
        self.assertEqual(len(detail_list), 1)

        d = detail_list[0]
        # Attribute access (not dict access) must work after round-trip
        self.assertEqual(bytes(d.workerManagerID), b"mgr_a")
        self.assertEqual(d.identity, "host1")
        self.assertEqual(d.lastSeenS, 5)
        self.assertEqual(d.maxTaskConcurrency, 4)
        self.assertEqual(d.capabilities, "gpu")
        self.assertEqual(d.pendingWorkers, 2)

    def test_worker_manager_id_not_empty(self):
        """Worker manager ID must not be empty bytes after round-trip."""
        sms = ScalingManagerStatus.from_bytes(
            build_scaling_manager_status(
                {b"local_a": [b"w1"]},
                [
                    {
                        "worker_manager_id": b"local_a",
                        "identity": "id",
                        "last_seen_s": 1,
                        "max_task_concurrency": 2,
                        "capabilities": "",
                        "pending_workers": 0,
                    }
                ],
            ).to_bytes()
        )
        for d in sms.workerManagerDetails:
            self.assertTrue(bytes(d.workerManagerID), "workerManagerID should not be empty")
            self.assertEqual(bytes(d.workerManagerID), b"local_a")

    def test_empty_managed_workers(self):
        sms = ScalingManagerStatus.from_bytes(build_scaling_manager_status({}).to_bytes())
        self.assertEqual(len(list(sms.managedWorkers)), 0)
        self.assertEqual(len(list(sms.workerManagerDetails)), 0)
