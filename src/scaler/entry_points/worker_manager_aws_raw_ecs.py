from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager


def main():
    ecs_config = ECSWorkerManagerConfig.parse("Scaler ECS Worker Manager", "ecs_worker_manager")

    setup_logger(
        ecs_config.logging_config.paths, ecs_config.logging_config.config_file, ecs_config.logging_config.level
    )

    ecs_worker_manager = ECSWorkerManager(ecs_config)
    ecs_worker_manager.run()


if __name__ == "__main__":
    main()
