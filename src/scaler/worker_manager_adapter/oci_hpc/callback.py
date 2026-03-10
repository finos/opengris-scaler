"""
OCI Container Instance Job Callback Handler.

Manages the mapping between task IDs and OCI Container Instance futures,
handling job completion and failure callbacks.

Analogous to the AWS Batch BatchJobCallback, but adapted for OCI Container Instances.
Each container instance is identified by its OCID (Oracle Cloud Identifier).
"""

import concurrent.futures
import logging
import threading
from typing import Any, Dict, List, Optional


class ContainerInstanceJobCallback:
    """
    Callback handler for OCI Container Instance job completions.

    Analogous to BatchJobCallback but adapted for OCI Container Instances'
    polling-based lifecycle model. Container instances are identified by
    their OCID (e.g., ``ocid1.containerinstance.oc1.us-ashburn-1.xxx``).
    """

    def __init__(self) -> None:
        self._callback_lock = threading.Lock()
        self._task_id_to_future: Dict[str, concurrent.futures.Future] = {}
        self._task_id_to_instance_id: Dict[str, str] = {}
        self._instance_id_to_task_id: Dict[str, str] = {}

    def on_job_succeeded(self, instance_id: str, result: Any) -> None:
        """
        Handle successful container instance completion.

        Args:
            instance_id: OCI Container Instance OCID
            result: Deserialized result fetched from OCI Object Storage
        """
        with self._callback_lock:
            task_id = self._instance_id_to_task_id.get(instance_id)
            if task_id is None:
                logging.warning(f"Received result for unknown container instance: {instance_id}")
                return

            future = self._task_id_to_future.pop(task_id, None)
            if future is None:
                logging.warning(f"No future found for task: {task_id}")
                return

            self._cleanup_instance_mapping(task_id, instance_id)

            if not future.done():
                future.set_result(result)

    def on_job_failed(self, instance_id: str, exception: Exception) -> None:
        """
        Handle container instance failure.

        Args:
            instance_id: OCI Container Instance OCID
            exception: Exception that caused the failure
        """
        with self._callback_lock:
            task_id = self._instance_id_to_task_id.get(instance_id)
            if task_id is None:
                logging.warning(f"Received failure for unknown container instance: {instance_id}")
                return

            future = self._task_id_to_future.pop(task_id, None)
            if future is None:
                logging.warning(f"No future found for task: {task_id}")
                return

            self._cleanup_instance_mapping(task_id, instance_id)

            if not future.done():
                future.set_exception(exception)

    def on_exception(self, exception: Exception) -> None:
        """
        Handle global exception affecting all pending tasks.

        Args:
            exception: Exception to propagate to all futures
        """
        with self._callback_lock:
            for task_id, future in list(self._task_id_to_future.items()):
                if not future.done():
                    future.set_exception(exception)

            self._task_id_to_future.clear()
            self._task_id_to_instance_id.clear()
            self._instance_id_to_task_id.clear()

    def submit_task(self, task_id: str, instance_id: str, future: concurrent.futures.Future) -> None:
        """
        Register a task submission for callback tracking.

        Args:
            task_id: Scaler task ID
            instance_id: OCI Container Instance OCID
            future: Future to resolve when the container instance completes
        """
        with self._callback_lock:
            self._task_id_to_future[task_id] = future
            self._task_id_to_instance_id[task_id] = instance_id
            self._instance_id_to_task_id[instance_id] = task_id

    def cancel_task(self, task_id: str) -> Optional[str]:
        """
        Cancel a task and return its container instance ID for deletion.

        Args:
            task_id: Scaler task ID to cancel

        Returns:
            OCI Container Instance OCID if found, None otherwise
        """
        with self._callback_lock:
            future = self._task_id_to_future.pop(task_id, None)
            instance_id = self._task_id_to_instance_id.pop(task_id, None)

            if instance_id:
                self._instance_id_to_task_id.pop(instance_id, None)

            if future and not future.done():
                future.cancel()

            return instance_id

    def get_instance_id(self, task_id: str) -> Optional[str]:
        """Get the OCI Container Instance OCID for a task."""
        with self._callback_lock:
            return self._task_id_to_instance_id.get(task_id)

    def get_pending_instance_ids(self) -> List[str]:
        """Get all pending OCI Container Instance OCIDs."""
        with self._callback_lock:
            return list(self._instance_id_to_task_id.keys())

    def get_callback_lock(self) -> threading.Lock:
        """Get the callback lock for external synchronization."""
        return self._callback_lock

    def _cleanup_instance_mapping(self, task_id: str, instance_id: str) -> None:
        """Clean up internal mappings after instance completion."""
        self._task_id_to_instance_id.pop(task_id, None)
        self._instance_id_to_task_id.pop(instance_id, None)
