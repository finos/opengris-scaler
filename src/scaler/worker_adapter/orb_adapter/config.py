import dataclasses
import pathlib

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class ORBWorkerAdapterConfig(ConfigClass):
    """Configuration for the Open Resource Broker (ORB) worker adapter.

    This adapter uses the open-resource-broker package to provision workers
    on Kubernetes clusters via ORB's file-based request system.

    The ORB processes (watch_pods, watch_requests, watch_return_requests)
    must be running separately before starting this adapter.
    """

    web_config: WebConfig
    worker_adapter_config: WorkerAdapterConfig

    orb_workdir: str = dataclasses.field(
        metadata=dict(short="-owd", help="path to the ORB working directory where requests and pod status are managed")
    )

    orb_templates: str = dataclasses.field(metadata=dict(short="-ot", help="path to the ORB pod templates JSON file"))

    # Fields with defaults must come after required fields
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    orb_template_id: str = dataclasses.field(
        default="default", metadata=dict(short="-oti", help="the template ID to use for worker pod creation")
    )

    workers_per_group: int = dataclasses.field(
        default=1, metadata=dict(short="-wpg", help="number of workers to create per worker group")
    )

    def __post_init__(self) -> None:
        if self.workers_per_group <= 0:
            raise ValueError("workers_per_group must be a positive integer.")

        # Validate paths exist
        workdir_path = pathlib.Path(self.orb_workdir)
        if not workdir_path.exists():
            raise ValueError(f"ORB workdir does not exist: {self.orb_workdir}")

        templates_path = pathlib.Path(self.orb_templates)
        if not templates_path.exists():
            raise ValueError(f"ORB templates file does not exist: {self.orb_templates}")
