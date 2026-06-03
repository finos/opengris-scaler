import dataclasses
from typing import Any, Dict

from scaler.utility.identifiers import TaskID, WorkerID


@dataclasses.dataclass
class InformationSnapshot:
    tasks: Dict[TaskID, Any]
    workers: Dict[WorkerID, Any]
