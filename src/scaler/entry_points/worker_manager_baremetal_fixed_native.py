import signal

from scaler.config.section.fixed_native_worker_adapter import FixedNativeWorkerAdapterConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.baremetal.fixed_native import FixedNativeWorkerAdapter


def main(section: str = "fixed_native_worker_adapter"):
    fixed_native_adapter_config = FixedNativeWorkerAdapterConfig.parse("Scaler Fixed Native Worker Adapter", section)

    register_event_loop(fixed_native_adapter_config.event_loop)

    setup_logger(
        fixed_native_adapter_config.logging_config.paths,
        fixed_native_adapter_config.logging_config.config_file,
        fixed_native_adapter_config.logging_config.level,
    )

    fixed_native_worker_adapter = FixedNativeWorkerAdapter(fixed_native_adapter_config)

    def handle_signal(signum, frame):
        fixed_native_worker_adapter.shutdown()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    fixed_native_worker_adapter.start()
    fixed_native_worker_adapter.join()


if __name__ == "__main__":
    main()
