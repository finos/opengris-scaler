from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.native import NativeWorkerAdapter


def main():
    native_adapter_config = NativeWorkerAdapterConfig.parse("Scaler Native Worker Adapter", "native_worker_adapter")

    setup_logger(
        native_adapter_config.logging_config.paths,
        native_adapter_config.logging_config.config_file,
        native_adapter_config.logging_config.level,
    )

    native_worker_adapter = NativeWorkerAdapter(native_adapter_config)

    native_worker_adapter.run()


if __name__ == "__main__":
    main()
