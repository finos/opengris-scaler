"""
This example demonstrates how to use capabilities with submit_verbose().

It shows how to route tasks to workers with specific capabilities (like GPU) using the capabilities routing feature.
"""

import math
import multiprocessing

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager


def gpu_task(x: float) -> float:
    """
    A task requiring the use of a GPU.
    """
    return math.sqrt(x) * 2


def cpu_task(x: float) -> float:
    """
    A regular CPU task.
    """
    return x * 2


def main():
    # Start a scheduler with the capabilities allocation policy, and a pair of regular workers.
    cluster = SchedulerClusterCombo(
        n_workers=2, scaler_policy=PolicyConfig(policy_content="allocate=capability; scaling=no")
    )

    # Adds an additional worker with GPU support
    base_manager = cluster._worker_manager
    gpu_manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=base_manager._address,
                object_storage_address=base_manager._object_storage_address,
                max_workers=1,
            ),
            mode=NativeWorkerManagerMode.FIXED,
            event_loop=base_manager._event_loop,
            worker_io_threads=base_manager._io_threads,
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                per_worker_task_queue_size=base_manager._task_queue_size,
                heartbeat_interval_seconds=base_manager._heartbeat_interval_seconds,
                task_timeout_seconds=base_manager._task_timeout_seconds,
                death_timeout_seconds=base_manager._death_timeout_seconds,
                garbage_collect_interval_seconds=base_manager._garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=base_manager._trim_memory_threshold_bytes,
                hard_processor_suspend=base_manager._hard_processor_suspend,
            ),
            logging_config=LoggingConfig(
                paths=base_manager._logging_paths,
                level=base_manager._logging_level,
                config_file=base_manager._logging_config_file,
            ),
        )
    )
    gpu_manager_process = multiprocessing.Process(target=gpu_manager.run)
    gpu_manager_process.start()

    with Client(address=cluster.get_address()) as client:
        print("Submitting tasks...")

        # Submit a task that requires GPU capabilities, this will be redirected to the GPU worker.
        gpu_future = client.submit_verbose(
            gpu_task, args=(16.0,), kwargs={}, capabilities={"gpu": 1}  # Requires a GPU capability
        )

        # Submit a task that does not require GPU capabilities, this will be routed to any available worker.
        cpu_future = client.submit_verbose(
            cpu_task, args=(16.0,), kwargs={}, capabilities={}  # No GPU capability required
        )

        # Waits for the tasks for finish
        gpu_future.result()
        cpu_future.result()

    if gpu_manager_process.is_alive():
        gpu_manager_process.terminate()
    gpu_manager_process.join()
    cluster.shutdown()


if __name__ == "__main__":
    main()
