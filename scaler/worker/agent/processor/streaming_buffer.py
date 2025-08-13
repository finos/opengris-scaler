import io
import logging
import threading

from scaler.io.sync_connector import SyncConnector
from scaler.protocol.python.message import TaskLog
from scaler.utility.identifiers import TaskID


class StreamingBuffer(io.StringIO):
    """A custom IO buffer that sends content as it's written."""

    def __init__(self, task_id: TaskID, log_type: TaskLog.LogType, connector_agent: SyncConnector):
        super().__init__()
        self._task_id = task_id
        self._log_type = log_type
        self._connector_agent = connector_agent
        self._lock = threading.Lock()

    def write(self, content: str) -> int:
        if self.closed:
            return 0

        with self._lock:
            # Write to parent StringIO buffer
            result = super().write(content)

            # Send content immediately if non-empty
            if content:
                try:
                    self._connector_agent.send(TaskLog.new_msg(self._task_id, self._log_type, content))
                except Exception as e:
                    logging.warning(f"Failed to send stream content: {e}")

            return result
