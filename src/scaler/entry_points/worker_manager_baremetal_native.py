from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager


def main():
    native_manager_config = NativeWorkerManagerConfig.parse("Scaler Native Worker Manager", "native_worker_manager")

    setup_logger(
        native_manager_config.logging_config.paths,
        native_manager_config.logging_config.config_file,
        native_manager_config.logging_config.level,
    )

    native_worker_manager = NativeWorkerManager(native_manager_config)

    native_worker_manager.run()


if __name__ == "__main__":
    main()
