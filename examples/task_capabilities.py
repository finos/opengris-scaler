"""
This example demonstrates how to use capabilities with submit_verbose().

It shows how to route tasks to workers with specific capabilities (like GPU) using the capabilities routing feature.
"""

import dataclasses
import math

from scaler import Client, Cluster
from scaler.cluster.combo import SchedulerClusterCombo
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy


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
    cluster = SchedulerClusterCombo(n_workers=2, allocate_policy=AllocatePolicy.capability)

    # Adds an additional worker with GPU support
    base_config = cluster._cluster._cluster_config
    cluster_config = dataclasses.replace(
        base_config,
        object_storage_address=None,
        preload=None,
        worker_names=WorkerNames(names=["gpu_worker"]),
        num_of_workers=1,
        per_worker_capabilities=WorkerCapabilities(capabilities={"gpu": -1}),
        worker_io_threads=1,
    )
    regular_cluster = Cluster(config=cluster_config)
    regular_cluster.start()

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

    cluster.shutdown()


if __name__ == "__main__":
    main()
