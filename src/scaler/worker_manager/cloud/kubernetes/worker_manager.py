from __future__ import annotations

import asyncio
import functools
import logging
import math
import shlex
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

import kubernetes
import kubernetes.client
import kubernetes.client.exceptions
import kubernetes.config
import kubernetes.watch
import yaml
from kubernetes.client import ApiClient, V1Container, V1ObjectMeta, V1Pod, V1PodSpec

from scaler.config.section.kubernetes_worker_manager import KubernetesWorkerManagerConfig
from scaler.utility.dict_utils import deep_merge
from scaler.worker_manager.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager.cloud.child_command import format_capabilities, load_file_or_inline
from scaler.worker_manager.desired_concurrency import extract_desired_count
from scaler.worker_manager.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager.runner import WorkerManagerRunner

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)

# With restartPolicy Never, a pod in one of these phases will never run its workers again.
EXITED_POD_PHASES = ("Failed", "Succeeded")

# Delay before replacing exited pods, as the kubelet's CrashLoopBackOff: doubles per exit, resets when quiet.
POD_EXIT_BACKOFF_INITIAL_SECONDS = 10.0
POD_EXIT_BACKOFF_MAX_SECONDS = 300.0
POD_EXIT_BACKOFF_RESET_SECONDS = 600.0


class KubernetesWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: KubernetesWorkerManagerConfig, max_pods: int) -> None:
        self._config = config
        self._capabilities: Dict[str, int] = config.worker_config.per_worker_capabilities.capabilities

        # Load Kubernetes auth
        if config.kubeconfig_path:
            kubernetes.config.load_kube_config(config.kubeconfig_path)
        else:
            kubernetes.config.load_incluster_config()
        self._core_v1 = kubernetes.client.CoreV1Api()

        # Pod names owned by this provisioner (oldest first)
        self._pods: List[str] = []

        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=max_pods,
        )

        self._exit_backoff_seconds = 0.0
        self._last_pod_exit_at = -math.inf
        self._starts_resume_at = 0.0
        self._backoff_reconcile_task: Optional[asyncio.Future] = None

        self._watch_stop = threading.Event()
        self._watch_thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Pod watch
    # ------------------------------------------------------------------

    def _start_pod_watch(self) -> None:
        if self._watch_thread is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._watch_thread = threading.Thread(target=self._pod_watch_loop, daemon=True)
        self._watch_thread.start()

    def _pod_watch_loop(self) -> None:
        label_selector = f"scaler/worker-manager-id={self._config.worker_manager_config.worker_manager_id}"
        watcher = kubernetes.watch.Watch()

        while not self._watch_stop.is_set():
            try:
                for event in watcher.stream(
                    self._core_v1.list_namespaced_pod,
                    self._config.namespace,
                    label_selector=label_selector,
                    timeout_seconds=300,
                ):
                    if self._watch_stop.is_set():
                        watcher.stop()
                        return

                    event_type = event["type"]
                    pod = event["object"]
                    pod_name = pod.metadata.name
                    if pod_name not in self._pods:
                        continue

                    if event_type == "DELETED":
                        logger.warning(f"Watch: pod {pod_name!r} was deleted externally, removing from tracking")
                        self._loop.call_soon_threadsafe(self._handle_external_pod_deletion, pod_name)
                    elif pod.status.phase in EXITED_POD_PHASES:  # MODIFIED, or ADDED when the watch reconnects
                        logger.warning(f"Watch: pod {pod_name!r} reached {pod.status.phase}: {_describe_exit(pod)}")
                        self._loop.call_soon_threadsafe(self._handle_exited_pod, pod_name)
            except kubernetes.client.exceptions.ApiException as e:
                if self._watch_stop.is_set():
                    return
                logger.error(f"Pod watch API error (will reconnect): {e}")
            except Exception as e:
                if self._watch_stop.is_set():
                    return
                logger.error(f"Pod watch error (will reconnect): {e}")

    def _handle_external_pod_deletion(self, pod_name: str) -> None:
        if pod_name in self._pods:
            self._pods.remove(pod_name)
            asyncio.ensure_future(self._capacity_coordinator.request_reconcile())

    def _handle_exited_pod(self, pod_name: str) -> None:
        # stop_units may have untracked the pod while the event was in flight.
        if pod_name in self._pods:
            self._pods.remove(pod_name)
            asyncio.ensure_future(self._delete_pod(pod_name))
            self._back_off_pod_starts()

    def _back_off_pod_starts(self) -> None:
        now = time.monotonic()
        if now - self._last_pod_exit_at > POD_EXIT_BACKOFF_RESET_SECONDS:
            self._exit_backoff_seconds = 0.0
        self._last_pod_exit_at = now
        self._exit_backoff_seconds = min(
            max(2 * self._exit_backoff_seconds, POD_EXIT_BACKOFF_INITIAL_SECONDS), POD_EXIT_BACKOFF_MAX_SECONDS
        )
        self._starts_resume_at = now + self._exit_backoff_seconds
        logger.warning(f"Delaying replacement pods by {self._exit_backoff_seconds:.0f}s after a worker pod exit")

        if self._backoff_reconcile_task is not None:
            self._backoff_reconcile_task.cancel()
        self._backoff_reconcile_task = asyncio.ensure_future(self._reconcile_after(self._exit_backoff_seconds))

    async def _reconcile_after(self, delay_seconds: float) -> None:
        await asyncio.sleep(delay_seconds)
        await self._capacity_coordinator.request_reconcile()

    def _stop_pod_watch(self) -> None:
        self._watch_stop.set()
        if self._watch_thread is not None:
            self._watch_thread.join(timeout=5)
            self._watch_thread = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_worker_command(self) -> str:
        config = self._config
        worker_config = config.worker_config
        scheduler_address = str(config.worker_manager_config.effective_worker_scheduler_address)

        command = (
            f"scaler_worker_manager baremetal_native {scheduler_address!r} "
            f"--mode fixed "
            f"--worker-type KUBERNETES "
            f"--max-task-concurrency {config.workers_per_pod} "
            f"--worker-manager-id {config.worker_manager_config.worker_manager_id} "
            f"--per-worker-task-queue-size {worker_config.per_worker_task_queue_size} "
            f"--heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} "
            f"--task-timeout-seconds {worker_config.task_timeout_seconds} "
            f"--garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} "
            f"--death-timeout-seconds {worker_config.death_timeout_seconds} "
            f"--trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} "
            f"--event-loop {worker_config.event_loop} "
            f"--io-threads {worker_config.io_threads}"
        )

        if worker_config.hard_processor_suspend:
            command += " --hard-processor-suspend"

        object_storage_address = config.worker_manager_config.object_storage_address
        if object_storage_address is not None:
            command += f" --object-storage-address {object_storage_address!r}"

        capabilities_str = format_capabilities(self._capabilities).strip()
        if capabilities_str:
            command += f" --per-worker-capabilities {capabilities_str}"

        if worker_config.preload is not None:
            command += f" --preload {shlex.quote(worker_config.preload)}"

        return command

    # ------------------------------------------------------------------
    # DeclarativeWorkerProvisioner interface
    # ------------------------------------------------------------------

    def active_unit_count(self) -> int:
        return len(self._pods)

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        self._start_pod_watch()
        task_concurrency = extract_desired_count(requests, self._capabilities)
        desired_pods = math.ceil(task_concurrency / self._config.workers_per_pod)
        await self._capacity_coordinator.set_desired_unit_count(desired_pods)

    async def start_units(self, count: int) -> None:
        # _reconcile_after requests another reconcile once the backoff ends.
        remaining_seconds = self._starts_resume_at - time.monotonic()
        if remaining_seconds > 0:
            logger.info(f"Holding {count} pod start(s) for {remaining_seconds:.0f}s after a worker pod exit")
            return

        config = self._config
        command = self._build_worker_command()
        loop = asyncio.get_running_loop()

        for _ in range(count):
            pod_name = f"scaler-worker-{uuid.uuid4().hex[:12]}"

            scaler_env: List[Dict[str, str]] = [{"name": "COMMAND", "value": command}]
            pwe = config.python_worker_environment
            if pwe.requirements_txt is not None:
                scaler_env.append({"name": "PYTHON_REQUIREMENTS", "value": load_file_or_inline(pwe.requirements_txt)})
            if pwe.python_version is not None:
                scaler_env.append({"name": "PYTHON_VERSION", "value": pwe.python_version})

            pod_manifest = V1Pod(
                metadata=V1ObjectMeta(
                    name=pod_name,
                    namespace=config.namespace,
                    labels={
                        "app": "scaler-worker",
                        "scaler/worker-manager-id": config.worker_manager_config.worker_manager_id,
                    },
                ),
                spec=V1PodSpec(
                    restart_policy="Never", containers=[V1Container(name="scaler-worker", image=config.pod_image)]
                ),
            )

            try:
                pod_dict: Dict[str, Any] = cast(Dict[str, Any], ApiClient().sanitize_for_serialization(pod_manifest))

                # Layer 1: pod_template YAML — deep-merged as a base layer.
                pod_template_content = load_file_or_inline(config.pod_template)
                if pod_template_content.strip():
                    template = yaml.safe_load(pod_template_content)
                    if not isinstance(template, dict):
                        raise ValueError(f"pod_template must be a YAML mapping, got {type(template).__name__}")
                    pod_dict = deep_merge(pod_dict, template)

                # Config fields override the template.
                spec = pod_dict.setdefault("spec", {})
                containers = spec.setdefault("containers", [{}])
                container = containers[0] if containers else {}
                if not containers:
                    containers.append(container)

                container["image"] = config.pod_image

                if config.node_selector:
                    spec["nodeSelector"] = config.node_selector

                if config.service_account_name:
                    spec["serviceAccountName"] = config.service_account_name

                if config.image_pull_policy:
                    container["imagePullPolicy"] = config.image_pull_policy

                if config.resource_requests or config.resource_limits:
                    resources = container.setdefault("resources", {})
                    if config.resource_requests:
                        resources["requests"] = config.resource_requests
                    if config.resource_limits:
                        resources["limits"] = config.resource_limits

                # Merge env vars: Scaler's vars override template vars by name.
                scaler_env_names = {e["name"] for e in scaler_env}
                template_env = [e for e in container.get("env", []) if e.get("name") not in scaler_env_names]
                container["env"] = template_env + scaler_env

                # Invariant: restartPolicy must always be Never.
                spec["restartPolicy"] = "Never"

                await loop.run_in_executor(
                    None,
                    functools.partial(self._core_v1.create_namespaced_pod, config.namespace, V1Pod.from_dict(pod_dict)),
                )
                self._pods.append(pod_name)
                logger.info(f"Started Kubernetes pod {pod_name!r}")
            except kubernetes.client.exceptions.ApiException as e:
                logger.error(f"Failed to create Kubernetes pod {pod_name!r}: {e}")

    async def stop_units(self, count: int) -> None:
        to_stop = self._pods[:count]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} pod(s) but only {len(to_stop)} available.")

        for pod_name in to_stop:
            if await self._delete_pod(pod_name) and pod_name in self._pods:
                self._pods.remove(pod_name)

    async def _delete_pod(self, pod_name: str) -> bool:
        """Delete *pod_name* and return whether it is gone."""
        config = self._config
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._core_v1.delete_namespaced_pod,
                    pod_name,
                    config.namespace,
                    grace_period_seconds=config.delete_grace_period_seconds,
                ),
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                logger.warning(f"Pod {pod_name!r} not found during deletion (already gone)")
                return True
            logger.error(f"Failed to delete pod {pod_name!r}: {e}")
            return False
        logger.info(f"Stopped Kubernetes pod {pod_name!r}")
        return True

    async def terminate(self) -> None:
        self._stop_pod_watch()
        if self._backoff_reconcile_task is not None:
            self._backoff_reconcile_task.cancel()
        self._capacity_coordinator.cancel()
        await self.stop_units(len(self._pods))


def _describe_exit(pod: V1Pod) -> str:
    for container_status in pod.status.container_statuses or []:
        terminated = container_status.state.terminated if container_status.state else None
        if terminated is not None:
            return f"container {container_status.name!r} exited with code {terminated.exit_code} ({terminated.reason})"
    return "no container exit status"


class KubernetesWorkerManager:
    def __init__(self, config: KubernetesWorkerManagerConfig) -> None:
        workers_per_pod = config.workers_per_pod
        mtc = config.worker_manager_config.max_task_concurrency
        max_pods = math.ceil(mtc / workers_per_pod) if mtc != -1 else -1
        provisioner = KubernetesWorkerProvisioner(config, max_pods)
        self._runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_k8s",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=max_pods,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=workers_per_pod,
        )

    def run(self) -> None:
        self._runner.run()
