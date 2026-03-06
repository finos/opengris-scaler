import dataclasses

from scaler.config.section.native_worker_adapter import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerAdapter


def main() -> None:
    config = NativeWorkerManagerConfig.parse("Scaler Cluster", "cluster")
    config = dataclasses.replace(config, mode=NativeWorkerManagerMode.FIXED)
    NativeWorkerAdapter(config).run()


__all__ = ["main"]
