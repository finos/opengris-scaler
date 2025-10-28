import io
import logging

from scaler.io.mixins import SyncConnector
from scaler.protocol.python.message import TaskLog
from scaler.utility.identifiers import TaskID


class StreamingBuffer(io.TextIOBase):
    """A custom IO buffer that sends content as it's written."""

    def __init__(self, task_id: TaskID, log_type: TaskLog.LogType, connector_agent: SyncConnector):
        super().__init__()
        self._task_id = task_id
        self._log_type = log_type
        self._connector_agent = connector_agent

    def write(self, content: str) -> int:
        if self.closed:
            return 0

        if content:
            try:
                self._connector_agent.send(TaskLog.new_msg(self._task_id, self._log_type, content))
            except Exception as e:
                logging.warning(f"Failed to send stream content: {e}")

        return 0
