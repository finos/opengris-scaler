from scaler.config.section.oci_raw_worker_adapter import OCIRawWorkerAdapterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.oci_raw.container_instance import OCIContainerInstanceWorkerAdapter


def main():
    config = OCIRawWorkerAdapterConfig.parse(
        "Scaler OCI Container Instance Worker Adapter", "oci_raw_worker_adapter"
    )

    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    adapter = OCIContainerInstanceWorkerAdapter(config)
    adapter.run()


if __name__ == "__main__":
    main()
