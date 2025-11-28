import dataclasses
from typing import Optional, Tuple
from urllib.parse import urlparse

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.scheduler.controllers.scaling_policies.types import ScalingControllerStrategy
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class SchedulerConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler address to connect to, e.g.: `tcp://localhost:6378`")
    )
    object_storage_address: Optional[ObjectStorageAddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-osa",
            help="specify the object storage server address, if not specified, "
            "the address is scheduler address with port number plus 1, "
            "e.g.: if scheduler address is tcp://localhost:2345, "
            "then object storage address is tcp://localhost:2346",
        ),
    )
    monitor_address: Optional[ZMQConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-ma",
            help="specify monitoring address, if not specified, the monitoring address is scheduler address with port "
            "number plus 2, e.g.: if scheduler address is tcp://localhost:2345, then monitoring address is "
            "tcp://localhost:2347",
        ),
    )
    scaling_controller_strategy: ScalingControllerStrategy = dataclasses.field(
        default=ScalingControllerStrategy.NULL,
        metadata=dict(
            short="-scs",
            choices=[s.name for s in ScalingControllerStrategy],
            help="specify the scaling controller strategy, if not specified, no scaling controller will be used",
        ),
    )
    adapter_webhook_urls: Tuple[str, ...] = dataclasses.field(
        default=(),
        metadata=dict(
            short="-awu",
            type=str,
            nargs="*",
            help="specify the adapter webhook urls for the scaling controller to send scaling events to",
        ),
    )
    protected: bool = dataclasses.field(
        default=False,
        metadata=dict(
            short="-p", action="store_true", help="protect scheduler and worker from being shutdown by client"
        ),
    )
    allocate_policy: AllocatePolicy = dataclasses.field(
        default=AllocatePolicy.even,
        metadata=dict(
            short="-ap",
            choices=[p.name for p in AllocatePolicy],
            help="specify allocate policy, this controls how scheduler will prioritize tasks, "
            "including balancing tasks",
        ),
    )
    max_number_of_tasks_waiting: int = dataclasses.field(
        default=defaults.DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        metadata=dict(short="-mt", help="max number of tasks can wait in scheduler while all workers are full"),
    )
    client_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_CLIENT_TIMEOUT_SECONDS,
        metadata=dict(short="-ct", help="discard client when timeout seconds reached"),
    )
    worker_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_TIMEOUT_SECONDS,
        metadata=dict(short="-wt", help="discard worker when timeout seconds reached"),
    )
    object_retention_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_OBJECT_RETENTION_SECONDS,
        metadata=dict(short="-ot", help="discard function in scheduler when timeout seconds reached"),
    )
    load_balance_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_LOAD_BALANCE_SECONDS,
        metadata=dict(short="-ls", help="number of seconds for load balance operation in scheduler"),
    )
    load_balance_trigger_times: int = dataclasses.field(
        default=defaults.DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        metadata=dict(
            short="-lbt",
            help="exact number of repeated load balance advices when trigger load balance operation in scheduler",
        ),
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-wit", help="set the number of io threads for io backend per worker"),
    )
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    def __post_init__(self):
        if self.max_number_of_tasks_waiting < -1:
            raise ValueError("max_number_of_tasks_waiting must be -1 (for unlimited) or non-negative.")
        if (
            self.client_timeout_seconds <= 0
            or self.worker_timeout_seconds <= 0
            or self.object_retention_seconds <= 0
            or self.load_balance_seconds <= 0
        ):
            raise ValueError("All timeout/retention/balance second values must be positive.")
        if self.load_balance_trigger_times <= 0:
            raise ValueError("load_balance_trigger_times must be a positive integer.")
        for adapter_webhook_url in self.adapter_webhook_urls:
            parsed_url = urlparse(adapter_webhook_url)
            if not all([parsed_url.scheme, parsed_url.netloc]):
                raise ValueError(f"adapter_webhook_urls contains url '{adapter_webhook_url}' which is not a valid URL.")
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
