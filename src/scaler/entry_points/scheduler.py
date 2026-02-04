from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig


def main():
    scheduler_config = SchedulerConfig.parse("Scaler Scheduler", "scheduler")

    object_storage_address = scheduler_config.object_storage_address
    object_storage = None

    if object_storage_address is None:
        assert scheduler_config.scheduler_address.port is not None, "Scheduler address must have a port"
        object_storage_address = ObjectStorageAddressConfig(
            host=scheduler_config.scheduler_address.host, port=scheduler_config.scheduler_address.port + 1
        )
        object_storage = ObjectStorageServerProcess(
            object_storage_address=object_storage_address,
            logging_paths=scheduler_config.logging_config.paths,
            logging_config_file=scheduler_config.logging_config.config_file,
            logging_level=scheduler_config.logging_config.level,
        )
        object_storage.start()
        object_storage.wait_until_ready()  # object storage should be ready before starting the cluster

    scheduler = SchedulerProcess(
        address=scheduler_config.scheduler_address,
        object_storage_address=object_storage_address,
        monitor_address=scheduler_config.monitor_address,
        io_threads=scheduler_config.worker_io_threads,
        max_number_of_tasks_waiting=scheduler_config.max_number_of_tasks_waiting,
        client_timeout_seconds=scheduler_config.client_timeout_seconds,
        worker_timeout_seconds=scheduler_config.worker_timeout_seconds,
        object_retention_seconds=scheduler_config.object_retention_seconds,
        load_balance_seconds=scheduler_config.load_balance_seconds,
        load_balance_trigger_times=scheduler_config.load_balance_trigger_times,
        protected=scheduler_config.protected,
        event_loop=scheduler_config.event_loop,
        policy=scheduler_config.policy,
        logging_paths=scheduler_config.logging_config.paths,
        logging_config_file=scheduler_config.logging_config.config_file,
        logging_level=scheduler_config.logging_config.level,
    )
    scheduler.start()

    scheduler.join()
    if object_storage is not None:
        object_storage.join()
