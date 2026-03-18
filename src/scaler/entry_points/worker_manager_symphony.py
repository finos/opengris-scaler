from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager


def main():
    symphony_config = SymphonyWorkerManagerConfig.parse("Scaler Symphony Worker Manager", "symphony_worker_manager")
    setup_logger(
        symphony_config.logging_config.paths,
        symphony_config.logging_config.config_file,
        symphony_config.logging_config.level,
    )

    symphony_worker_manager = SymphonyWorkerManager(symphony_config)
    symphony_worker_manager.run()


if __name__ == "__main__":
    main()
