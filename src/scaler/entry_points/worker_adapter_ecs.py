from aiohttp import web

from scaler.config.section.ecs_worker_adapter import ECSWorkerAdapterConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.ecs import ECSWorkerAdapter


def main():
    ecs_config = ECSWorkerAdapterConfig.parse("Scaler ECS Worker Adapter", "ecs_worker_adapter")
    register_event_loop(ecs_config.event_loop)
    setup_logger(
        ecs_config.logging_config.paths, ecs_config.logging_config.config_file, ecs_config.logging_config.level
    )
    ecs_worker_adapter = ECSWorkerAdapter(ecs_config)

    app = ecs_worker_adapter.create_app()
    web.run_app(app, host=ecs_config.web_config.adapter_web_host, port=ecs_config.web_config.adapter_web_port)


if __name__ == "__main__":
    main()
