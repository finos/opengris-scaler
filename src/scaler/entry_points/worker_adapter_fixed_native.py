from scaler.config.section.fixed_native_worker_adapter import FixedNativeWorkerAdapterConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.fixed_native import FixedNativeWorkerAdapter


def main():
    fixed_native_adapter_config = FixedNativeWorkerAdapterConfig.parse(
        "Scaler Fixed Native Worker Adapter", "fixed_native_worker_adapter"
    )

    register_event_loop(fixed_native_adapter_config.event_loop)

    setup_logger(
        fixed_native_adapter_config.logging_config.paths,
        fixed_native_adapter_config.logging_config.config_file,
        fixed_native_adapter_config.logging_config.level,
    )

    fixed_native_worker_adapter = FixedNativeWorkerAdapter(fixed_native_adapter_config)

    fixed_native_worker_adapter.start()
    fixed_native_worker_adapter.join()


if __name__ == "__main__":
    main()
