import logging
import multiprocessing
import os
import time
from typing import Optional

from aiohttp import web

from scaler.client.client import ScalerClient
from scaler.cluster.scheduler import SchedulerProcess
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.section.orb_worker_adapter import ORBWorkerAdapterConfig
from scaler.config.section.scheduler import SchedulerConfig, PolicyConfig
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.orb.worker_adapter import ORBAdapter

# Configuration Constants
SCHEDULER_HOST = "127.0.0.1"
SCHEDULER_PORT = 8786
ADAPTER_HOST = "127.0.0.1"
ADAPTER_PORT = 8888
ORB_CONFIG_PATH = os.path.abspath("orb_config.json")

# Dummy values for mandatory fields - USER MUST REPLACE THESE or set via ENV
# IMAGE_ID = os.environ.get("ORB_IMAGE_ID", "ami-12345678")
# SUBNET_ID = os.environ.get("ORB_SUBNET_ID", "subnet-12345678")
# SECURITY_GROUP_IDS = os.environ.get("ORB_SG_IDS", "sg-12345678")

IMAGE_ID = "ami-0528819f94f4f5fa5"
SUBNET_ID = None
SECURITY_GROUP_IDS = None

def run_scheduler(scheduler_config: SchedulerConfig):
    setup_logger(scheduler_config.logging_config.paths, scheduler_config.logging_config.config_file, "INFO")
    
    object_storage_address = scheduler_config.object_storage_address
    object_storage = None

    if object_storage_address is None:
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
        object_storage.wait_until_ready()

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
    if object_storage:
        object_storage.join()


def run_adapter(adapter_config: ORBWorkerAdapterConfig):
    setup_logger(adapter_config.logging_config.paths, adapter_config.logging_config.config_file, "INFO")
    register_event_loop(adapter_config.event_loop)

    try:
        adapter = ORBAdapter(adapter_config)
        app = adapter.create_app()
        web.run_app(
            app,
            host=adapter_config.web_config.adapter_web_host,
            port=adapter_config.web_config.adapter_web_port,
            print=None  # suppress startup message
        )
    except Exception as e:
        logging.exception(f"Adapter failed: {e}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.info("Starting OpenGRIS ORB Test Application")

    # 1. Configure Scheduler
    scheduler_address = ZMQConfig(host=SCHEDULER_HOST, port=SCHEDULER_PORT, type="tcp")
    scheduler_config = SchedulerConfig(
        scheduler_address=scheduler_address,
        policy=PolicyConfig(
            policy_content="scaling=yes",  # Enable scaling
            adapter_webhook_urls=(f"http://{ADAPTER_HOST}:{ADAPTER_PORT}",)
        ),
        logging_config=LoggingConfig()
    )

    # 2. Configure ORB Adapter
    adapter_config = ORBWorkerAdapterConfig(
        web_config=WebConfig(adapter_web_host=ADAPTER_HOST, adapter_web_port=ADAPTER_PORT),
        worker_adapter_config=WorkerAdapterConfig(
            scheduler_address=scheduler_address,
            max_workers=2  # Limit to 2 workers for testing
        ),
        orb_config_path=ORB_CONFIG_PATH,
        image_id=IMAGE_ID,
        subnet_id=SUBNET_ID,
        security_group_ids=[SECURITY_GROUP_IDS],
        instance_type="t2.small",
        logging_config=LoggingConfig(),
        aws_region="us-east-1"
    )

    # 3. Start Processes
    scheduler_proc = multiprocessing.Process(target=run_scheduler, args=(scheduler_config,))
    adapter_proc = multiprocessing.Process(target=run_adapter, args=(adapter_config,))

    try:
        logging.info("Starting Scheduler...")
        scheduler_proc.start()
        time.sleep(2)  # Wait for scheduler to be ready

        logging.info("Starting ORB Adapter...")
        adapter_proc.start()
        time.sleep(2)  # Wait for adapter to be ready

        # 4. Run Client
        logging.info(f"Connecting Client to {scheduler_address.to_address()}...")
        client = ScalerClient(scheduler_address.to_address())
        
        def simple_task(x: int) -> str:
            import socket
            return f"Processed {x} on {socket.gethostname()}"

        logging.info("Submitting tasks...")
        futures = [client.submit(simple_task, i) for i in range(5)]
        
        logging.info("Waiting for results (this triggers scaling)...")
        # Timeout 5 minutes to allow for instance launch
        for i, future in enumerate(futures):
            try:
                result = future.result(timeout_seconds=300) 
                logging.info(f"Task {i} Result: {result}")
            except Exception as e:
                logging.error(f"Task {i} failed or timed out: {e}")

    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    finally:
        logging.info("Shutting down...")
        if client:
            client.disconnect()
        
        if adapter_proc.is_alive():
            adapter_proc.terminate()
            adapter_proc.join()
        
        if scheduler_proc.is_alive():
            scheduler_proc.terminate()
            scheduler_proc.join()
        
        logging.info("Test completed.")

if __name__ == "__main__":
    main()
