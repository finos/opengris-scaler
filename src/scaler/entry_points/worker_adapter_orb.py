from aiohttp import web

from scaler.config.section.orb_worker_adapter import ORBWorkerAdapterConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.orb.worker_adapter import ORBAdapter


def main():
    orb_adapter_config = ORBWorkerAdapterConfig.parse("Scaler ORB Worker Adapter", "orb_worker_adapter")

    register_event_loop(orb_adapter_config.event_loop)

    setup_logger(
        orb_adapter_config.logging_config.paths,
        orb_adapter_config.logging_config.config_file,
        orb_adapter_config.logging_config.level,
    )

    orb_worker_adapter = ORBAdapter(orb_adapter_config)

    app = orb_worker_adapter.create_app()
    web.run_app(
        app, host=orb_adapter_config.web_config.adapter_web_host, port=orb_adapter_config.web_config.adapter_web_port
    )


if __name__ == "__main__":
    main()
