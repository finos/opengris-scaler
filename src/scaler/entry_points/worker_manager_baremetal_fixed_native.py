from scaler.config.section.fixed_native_worker_manager import FixedNativeWorkerManagerConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.baremetal.fixed_native import FixedNativeWorkerManager


def main():
    fixed_native_manager_config = FixedNativeWorkerManagerConfig.parse(
        "Scaler Fixed Native Worker Manager", "fixed_native_worker_manager"
    )

    register_event_loop(fixed_native_manager_config.event_loop)

    setup_logger(
        fixed_native_manager_config.logging_config.paths,
        fixed_native_manager_config.logging_config.config_file,
        fixed_native_manager_config.logging_config.level,
    )

    fixed_native_worker_manager = FixedNativeWorkerManager(fixed_native_manager_config)

    fixed_native_worker_manager.start()
    fixed_native_worker_manager.join()


if __name__ == "__main__":
    main()
