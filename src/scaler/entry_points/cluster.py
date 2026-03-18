import dataclasses

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager


def main() -> None:
    config = NativeWorkerManagerConfig.parse("Scaler Cluster", "cluster")
    config = dataclasses.replace(config, mode=NativeWorkerManagerMode.FIXED)
    NativeWorkerManager(config).run()


__all__ = ["main"]
