"""
This example shows how to instantiate a Cluster using the Python API.
For an example on how to instantiate a Scheduler, see simple_scheduler.py
"""

from scaler import Cluster
from scaler.io.config import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.zmq_config import ZMQConfig


def main():
    N_WORKERS = 8
    # Initialize a Cluster.
    cluster = Cluster(
        worker_io_threads=1,
        address=ZMQConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"),
        worker_names=[str(i) for i in range(N_WORKERS - 1)],
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
        death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
        garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
        event_loop="builtin",  # Or "uvloop"
        logging_paths=("/dev/stdout",),
        logging_level="DEBUG",  # other choices are "INFO", "WARNING", "ERROR", "CRITICAL"
        logging_config_file=None,
    )

    # Start the cluster. The cluster will begin accepting tasks from the scheduler.
    cluster.start()

    # Shut down the cluster. Cluster subclasses Process and can be shutdown using `.terminate()`, or arbitrary signals
    # can be sent using `.kill()`
    cluster.terminate()

    # Wait for the cluster's process to terminate.
    cluster.join()

    # Release resources. Must be called after `.join()` has returned.
    cluster.close()


if __name__ == "__main__":
    main()
