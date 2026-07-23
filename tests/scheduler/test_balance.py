import dataclasses
import multiprocessing
import os
import signal
import sys
import tempfile
import time
import unittest
from concurrent.futures import Future
from concurrent.futures import wait as wait_futures
from typing import List

import psutil

from scaler import Client, SchedulerClusterCombo
from scaler.config.defaults import DEFAULT_LOAD_BALANCE_SECONDS
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager
from tests.utility.utility import logging_test_name


def sleep_and_return_pid(sec: int):
    time.sleep(sec)
    return os.getpid()


def _signal_process_tree(process: multiprocessing.process.BaseProcess, sig: signal.Signals):
    """Send a signal to an entire process tree"""

    try:
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)
    except psutil.NoSuchProcess:
        return

    for member in [parent, *children]:
        try:
            member.send_signal(sig)
        except psutil.NoSuchProcess:
            pass


def _wait_for_log_marker(log_path: str, marker: str, timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        with open(log_path, "r") as log_file:
            if marker in log_file.read():
                return True
        time.sleep(0.2)
    return False


class TestBalance(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_balance(self):
        """
        Schedules a few long-lasting tasks to a single process cluster, then adds workers. We expect the remaining tasks
        to be balanced to the new workers.
        """

        N_TASKS = 8
        N_WORKERS = N_TASKS

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address,
            n_workers=1,
            per_worker_task_queue_size=N_TASKS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
        )

        client = Client(address=address)

        futures = [client.submit(sleep_and_return_pid, 10) for _ in range(N_TASKS)]

        time.sleep(3)

        base_config = combo._worker_manager.config
        new_manager = NativeWorkerManager(
            dataclasses.replace(
                base_config,
                worker_manager_config=dataclasses.replace(
                    base_config.worker_manager_config,
                    worker_manager_id="test_manager",
                    object_storage_address=None,
                    max_task_concurrency=N_WORKERS - 1,
                ),
            )
        )
        process = multiprocessing.get_context("spawn").Process(target=new_manager.run)
        process.start()

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS)

        client.disconnect()

        process.terminate()
        process.join()
        combo.shutdown()

    @unittest.skipIf(sys.platform == "win32", "freezes a worker's process tree via SIGSTOP/psutil, which is POSIX-only")
    def test_balance_cancel_worker_death(self):
        """
        Regression test for https://github.com/finos/opengris-scaler/pull/883.

        When using the ec2 worker manager, workers are killed when the instance shuts down.
        There was a bug in the scheduler that meant tasks would become stuck permanently in the balanceCanceling state.

        This integration test simulates those conditions by using SIGSTOP to freeze a worker right after
        the balance-cancel command is sent to it.
        """

        TASK_SLEEP_SECONDS = 5
        WORKER_TIMEOUT_SECONDS = 4
        HEARTBEAT_INTERVAL_SECONDS = 1

        log_fd, log_path = tempfile.mkstemp(prefix="scaler_balance_cancel_worker_death_", suffix=".log")
        os.close(log_fd)

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address,
            n_workers=1,
            # A single-task worker has nothing "excess" to balance away (see
            # even_load_allocate_policy.py), so it needs two tasks queued to be a candidate.
            per_worker_task_queue_size=2,
            worker_timeout_seconds=WORKER_TIMEOUT_SECONDS,
            heartbeat_interval_seconds=HEARTBEAT_INTERVAL_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=1,
            logging_paths=(log_path,),
        )

        client = Client(address=address)
        second_manager_process = None
        try:
            futures: List[Future] = [
                client.submit(sleep_and_return_pid, TASK_SLEEP_SECONDS),
                client.submit(sleep_and_return_pid, TASK_SLEEP_SECONDS),
            ]

            time.sleep(2)  # let worker_a register both tasks before it's frozen

            _signal_process_tree(combo._worker_manager_process, signal.SIGSTOP)  # type: ignore[attr-defined]

            base_config = combo._worker_manager.config
            second_manager = NativeWorkerManager(
                dataclasses.replace(
                    base_config,
                    worker_manager_config=dataclasses.replace(
                        base_config.worker_manager_config,
                        worker_manager_id="test_manager",
                        object_storage_address=None,
                        max_task_concurrency=2,  # both tasks can run concurrently once rerouted
                    ),
                )
            )
            second_manager_process = multiprocessing.get_context("spawn").Process(target=second_manager.run)
            second_manager_process.start()

            if not _wait_for_log_marker(log_path, "balancing task", 15.0):
                self.fail(
                    "balance controller never advised moving a task off worker_a -- "
                    "test setup did not reach the scenario under test"
                )

            done, not_done = wait_futures(futures, timeout=WORKER_TIMEOUT_SECONDS + TASK_SLEEP_SECONDS + 15)
            if not_done:
                self.fail(
                    "a task never completed: it was orphaned in balanceCanceling instead of being "
                    "rerouted after worker_a died mid-cancel"
                )

            for future in done:
                self.assertIsInstance(future.result(), int)
        finally:
            if second_manager_process is not None:
                second_manager_process.terminate()
                second_manager_process.join()

            # SIGKILL, not terminate()/SIGTERM: worker_a may still be frozen and SIGTERM would
            # never be delivered to a stopped process.
            _signal_process_tree(combo._worker_manager_process, signal.SIGKILL)  # type: ignore[attr-defined]
            combo._worker_manager_process.join()

            # Shut the scheduler down before disconnecting the client: if a task is still
            # orphaned, client.disconnect() blocks forever waiting for a cancel confirm that a
            # dead scheduler can never send.
            combo.shutdown()
            client.disconnect()

            try:
                os.remove(log_path)
            except OSError:
                pass
