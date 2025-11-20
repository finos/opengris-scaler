from aiohttp import web

from scaler.config.section.symphony_worker_adapter import SymphonyWorkerConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.symphony.worker_adapter import SymphonyWorkerAdapter


def main():
    symphony_config = SymphonyWorkerConfig.parse("Scaler Symphony Worker Adapter", "symphony_worker_adapter")
    register_event_loop(symphony_config.event_loop)

    setup_logger(
        symphony_config.logging_config.paths,
        symphony_config.logging_config.config_file,
        symphony_config.logging_config.level,
    )

    symphony_worker_adapter = SymphonyWorkerAdapter(symphony_config)

    app = symphony_worker_adapter.create_app()
    web.run_app(app, host=symphony_config.web_config.adapter_web_host, port=symphony_config.web_config.adapter_web_port)


if __name__ == "__main__":
    main()
