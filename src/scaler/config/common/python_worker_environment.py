import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class PythonWorkerEnvironmentConfig(ConfigClass):
    python_version: Optional[str] = dataclasses.field(
        default=None, metadata=dict(help="Python version for the worker environment (e.g. '3.12')")
    )
    requirements_txt: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            help=(
                "Requirements for each worker. Can be a path to a requirements.txt file or an inline string "
                "(newline-separated). Must include opengris-scaler."
            )
        ),
    )
