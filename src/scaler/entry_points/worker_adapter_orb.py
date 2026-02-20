from scaler.config.section.orb_worker_adapter import ORBWorkerAdapterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.drivers.orb_common.worker_adapter import ORBWorkerAdapter


def main():
    orb_adapter_config = ORBWorkerAdapterConfig.parse("Scaler ORB Worker Adapter", "orb_worker_adapter")

    setup_logger(
        orb_adapter_config.logging_config.paths,
        orb_adapter_config.logging_config.config_file,
        orb_adapter_config.logging_config.level,
    )

    orb_worker_adapter = ORBWorkerAdapter(orb_adapter_config)
    orb_worker_adapter.run()


if __name__ == "__main__":
    main()
