import array
import concurrent.futures
import threading
from typing import Dict

import cloudpickle

try:
    import soamapi
except ImportError:
    raise ImportError("IBM Spectrum Symphony API not found, please install it with 'pip install soamapi'.")


class SoamMessage(soamapi.Message):
    def __init__(self, payload: bytes = b""):
        self.__payload = payload

    def set_payload(self, payload: bytes):
        self.__payload = payload

    def get_payload(self) -> bytes:
        return self.__payload

    def on_serialize(self, stream):
        payload_array = array.array("b", self.get_payload())
        stream.write_byte_array(payload_array, 0, len(payload_array))

    def on_deserialize(self, stream):
        self.set_payload(stream.read_byte_array("b"))


class SessionCallback(soamapi.SessionCallback):
    def __init__(self):
        self._callback_lock = threading.Lock()
        self._task_id_to_future: Dict[str, concurrent.futures.Future] = {}

    def on_response(self, task_output_handle):
        with self._callback_lock:
            task_id = task_output_handle.get_id()

            future = self._task_id_to_future.pop(task_id)

            if task_output_handle.is_successful():
                output_message = SoamMessage()
                task_output_handle.populate_task_output(output_message)
                result = cloudpickle.loads(output_message.get_payload())
                future.set_result(result)
            else:
                future.set_exception(task_output_handle.get_exception().get_embedded_exception())

    def on_exception(self, exception):
        with self._callback_lock:
            for future in self._task_id_to_future.values():
                future.set_exception(exception)

            self._task_id_to_future.clear()

    def submit_task(self, task_id: str, future: concurrent.futures.Future):
        self._task_id_to_future[task_id] = future

    def get_callback_lock(self) -> threading.Lock:
        return self._callback_lock
