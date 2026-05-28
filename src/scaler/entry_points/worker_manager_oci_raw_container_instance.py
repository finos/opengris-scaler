from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.oci_raw.container_instance import OCIContainerInstanceWorkerManager


def main():
    config = OCIRawWorkerManagerConfig.parse("Scaler OCI Container Instance Worker Manager", "oci_raw_worker_manager")

    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    manager = OCIContainerInstanceWorkerManager(config)
    manager.run()


if __name__ == "__main__":
    main()
