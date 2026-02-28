from scaler.config.section.ecs_worker_adapter import ECSWorkerAdapterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerAdapter


def main():
    ecs_config = ECSWorkerAdapterConfig.parse("Scaler ECS Worker Adapter", "ecs_worker_adapter")

    setup_logger(
        ecs_config.logging_config.paths, ecs_config.logging_config.config_file, ecs_config.logging_config.level
    )

    ecs_worker_adapter = ECSWorkerAdapter(ecs_config)
    ecs_worker_adapter.run()


if __name__ == "__main__":
    main()
