from aiohttp import web

from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter
from scaler.worker_adapter.orb_adapter.config import ORBWorkerAdapterConfig


def main():
    orb_adapter_config = ORBWorkerAdapterConfig.parse("Scaler ORB Worker Adapter", "orb_worker_adapter")

    register_event_loop("builtin")

    setup_logger(
        orb_adapter_config.logging_config.paths,
        orb_adapter_config.logging_config.config_file,
        orb_adapter_config.logging_config.level,
    )

    orb_worker_adapter = ORBWorkerAdapter(orb_adapter_config)

    app = orb_worker_adapter.create_app()
    web.run_app(
        app, host=orb_adapter_config.web_config.adapter_web_host, port=orb_adapter_config.web_config.adapter_web_port
    )


if __name__ == "__main__":
    main()
