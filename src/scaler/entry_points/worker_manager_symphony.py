from scaler.config.section.symphony_worker_adapter import SymphonyWorkerConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.symphony.worker_adapter import SymphonyWorkerAdapter


def main():
    symphony_config = SymphonyWorkerConfig.parse("Scaler Symphony Worker Adapter", "symphony_worker_adapter")
    setup_logger(
        symphony_config.logging_config.paths,
        symphony_config.logging_config.config_file,
        symphony_config.logging_config.level,
    )

    symphony_worker_adapter = SymphonyWorkerAdapter(symphony_config)
    symphony_worker_adapter.run()


if __name__ == "__main__":
    main()
