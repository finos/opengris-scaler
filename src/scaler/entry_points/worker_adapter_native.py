from aiohttp import web

from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.native import NativeWorkerAdapter


def main():
    native_adapter_config = NativeWorkerAdapterConfig.parse("Scaler Native Worker Adapter", "native_worker_adapter")

    register_event_loop(native_adapter_config.event_loop)

    setup_logger(
        native_adapter_config.logging_config.paths,
        native_adapter_config.logging_config.config_file,
        native_adapter_config.logging_config.level,
    )

    native_worker_adapter = NativeWorkerAdapter(native_adapter_config)

    app = native_worker_adapter.create_app()
    web.run_app(
        app,
        host=native_adapter_config.web_config.adapter_web_host,
        port=native_adapter_config.web_config.adapter_web_port,
    )


if __name__ == "__main__":
    main()
