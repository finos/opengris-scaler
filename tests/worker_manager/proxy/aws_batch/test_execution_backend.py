import unittest
from unittest.mock import MagicMock

from scaler.protocol.capnp import TaskCancel
from scaler.utility.identifiers import TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.proxy.aws_batch.execution_backend import AWSBatchExecutionBackend
from tests.utility.utility import logging_test_name

BATCH_JOB_ID = "11111111-2222-3333-4444-555555555555"


def _force_cancel(task_id: TaskID) -> TaskCancel:
    return TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=True))


class TestAWSBatchExecutionBackendCancel(unittest.IsolatedAsyncioTestCase):
    """Pins that a cancel whose terminate_job fails raises, so the task manager confirms cancelFailed."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = AWSBatchExecutionBackend(
            job_queue="queue", job_definition="definition", aws_region="us-east-1", s3_bucket="bucket"
        )
        self.addCleanup(self.backend._executor.shutdown, wait=True)
        self.backend._batch_client = MagicMock()
        self.task_id = TaskID.generate_task_id()
        self.backend._task_id_to_batch_job_id[self.task_id] = BATCH_JOB_ID

    async def test_a_cancel_terminates_the_job(self) -> None:
        await self.backend.on_cancel(_force_cancel(self.task_id))

        self.backend._batch_client.terminate_job.assert_called_once()
        self.assertEqual(self.backend._batch_client.terminate_job.call_args.kwargs["jobId"], BATCH_JOB_ID)

    async def test_a_cancel_whose_terminate_job_fails_raises_and_keeps_the_job(self) -> None:
        self.backend._batch_client.terminate_job.side_effect = RuntimeError("terminate_job failed")

        with self.assertRaises(RuntimeError):
            await self.backend.on_cancel(_force_cancel(self.task_id))

        self.assertEqual(self.backend._task_id_to_batch_job_id[self.task_id], BATCH_JOB_ID)
