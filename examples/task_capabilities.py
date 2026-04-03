"""Route tasks to workers by capability with submit_verbose."""

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
    return math.sqrt(x) * 2


def cpu_task(x: float) -> float:
    return x * 2


def main():
    cluster = SchedulerClusterCombo(
        n_workers=2, scaler_policy=PolicyConfig(policy_content="allocate=capability; scaling=no")
    )

    base_manager = cluster._worker_manager
    gpu_manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=base_manager._address,
                worker_manager_id="test_manager",
                object_storage_address=base_manager._object_storage_address,
                max_task_concurrency=1,
            ),
            mode=NativeWorkerManagerMode.FIXED,
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                per_worker_task_queue_size=base_manager._task_queue_size,
                heartbeat_interval_seconds=base_manager._heartbeat_interval_seconds,
                task_timeout_seconds=base_manager._task_timeout_seconds,
                death_timeout_seconds=base_manager._death_timeout_seconds,
                garbage_collect_interval_seconds=base_manager._garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=base_manager._trim_memory_threshold_bytes,
                hard_processor_suspend=base_manager._hard_processor_suspend,
                io_threads=base_manager._io_threads,
                event_loop=base_manager._event_loop,
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
        gpu_future = client.submit_verbose(gpu_task, args=(16.0,), kwargs={}, capabilities={"gpu": 1})
        cpu_future = client.submit_verbose(cpu_task, args=(16.0,), kwargs={}, capabilities={})
        gpu_future.result()
        cpu_future.result()

    if gpu_manager_process.is_alive():
        gpu_manager_process.terminate()
    gpu_manager_process.join()
    cluster.shutdown()


if __name__ == "__main__":
    main()
