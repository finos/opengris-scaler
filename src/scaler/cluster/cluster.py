import asyncio
import logging
import multiprocessing
import signal

from aiohttp import web

from scaler.config.common.web import WebConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.section.cluster import ClusterConfig
from scaler.config.section.fixed_native_worker_adapter import FixedNativeWorkerAdapterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_adapter.fixed_native import FixedNativeWorkerAdapter


class Cluster(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(self, config: ClusterConfig):
        multiprocessing.Process.__init__(self, name="WorkerMaster")

        self._address = config.scheduler_address
        self._object_storage_address = config.object_storage_address
        self._preload = config.preload
        self._worker_io_threads = config.worker_io_threads
        self._worker_names = config.worker_names.names
        self._per_worker_capabilities = config.worker_config.per_worker_capabilities.capabilities

        self._per_worker_task_queue_size = config.worker_config.per_worker_task_queue_size
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.event_loop

        self._logging_paths = config.logging_config.paths
        self._logging_config_file = config.logging_config.config_file
        self._logging_level = config.logging_config.level
        self._deterministic_worker_ids = config.deterministic_worker_ids

        self._worker_adapter = FixedNativeWorkerAdapter(
            FixedNativeWorkerAdapterConfig(
                web_config=WebConfig(adapter_web_host=None, adapter_web_port=None),
                preload=config.preload,
                worker_adapter_config=WorkerAdapterConfig(
                    scheduler_address=config.scheduler_address,
                    object_storage_address=config.object_storage_address,
                    max_workers=len(config.worker_names),
                ),
                worker_config=config.worker_config,
                logging_config=config.logging_config,
                event_loop=config.event_loop,
                worker_io_threads=config.worker_io_threads,
            )
        )

    def run(self):
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)

        self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self._run())

    async def _run(self):
        self._stopped = asyncio.Event()

        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

        await self.__start_workers_and_run_forever()

    def __destroy(self):
        logging.info(f"{self.__get_prefix()} received signal, shutting down")

        # set the stopped event to exit the main loop
        self._loop.call_soon_threadsafe(self._stopped.set)

    async def __start_workers_and_run_forever(self):
        logging.info(
            f"{self.__get_prefix()} starting {len(self._worker_names)} workers, heartbeat_interval_seconds="
            f"{self._heartbeat_interval_seconds}, task_timeout_seconds={self._task_timeout_seconds}"
        )

        self._worker_adapter.start()
        app = self._worker_adapter.create_app()

        # the fixed native worker adapter doesn't actually support spawning and shutting down workers dynamically,
        # but we run the server here to keep the interface consistent
        # it may become useful in the future
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", get_available_tcp_port(), reuse_address=True, reuse_port=True)
        await site.start()

        logging.info(f"{self.__get_prefix()} started worker adapter web server on {site._host}:{site._port}")

        # run until stopped
        try:
            stop_task = asyncio.create_task(self._stopped.wait())

            # this is a blocking call, so we must run it in the executor
            join_task = self._loop.run_in_executor(None, self._worker_adapter.join)

            # we're done when either all the workers have exited, or we received a stop signal
            await asyncio.wait([stop_task, join_task], return_when=asyncio.FIRST_COMPLETED)
        finally:
            # stop the web server
            await runner.cleanup()
            # shut down all workers
            self._worker_adapter.shutdown()

        logging.info(f"{self.__get_prefix()} shutdown")

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"

    def shutdown(self):
        self.terminate()
        self.join()
