import time
from math import ceil
from typing import Dict, List, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class VanillaScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that scales workers based on task-to-worker ratio.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10
        print(
            f"[VANILLA-SCALING][INIT] VanillaScalingPolicy created. "
            f"lower_task_ratio={self._lower_task_ratio}, upper_task_ratio={self._upper_task_ratio}"
        )

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        manager_id = getattr(worker_manager_heartbeat, "workerManagerID", b"?")
        print(
            f"[VANILLA-SCALING][get_scaling_commands] ts={time.time():.3f} "
            f"manager_id={manager_id!r} "
            f"managed_workers={len(managed_worker_ids)} "
            f"total_tasks={len(information_snapshot.tasks)} "
            f"total_workers={len(information_snapshot.workers)} "
            f"max_task_concurrency={worker_manager_heartbeat.maxTaskConcurrency} "
            f"num_other_managers={len(worker_manager_snapshots)}"
        )
        print(
            f"[VANILLA-SCALING][get_scaling_commands]   managed_worker_ids={[bytes(w).hex() for w in managed_worker_ids]}"
        )
        print(
            f"[VANILLA-SCALING][get_scaling_commands]   task_ids={[bytes(t).hex() for t in list(information_snapshot.tasks.keys())[:20]]}"
            + (f" ... ({len(information_snapshot.tasks)} total)" if len(information_snapshot.tasks) > 20 else "")
        )
        print(
            f"[VANILLA-SCALING][get_scaling_commands]   worker_ids={[bytes(w).hex() for w in list(information_snapshot.workers.keys())[:20]]}"
            + (f" ... ({len(information_snapshot.workers)} total)" if len(information_snapshot.workers) > 20 else "")
        )

        desired = self._compute_desired_worker_count(information_snapshot, worker_manager_heartbeat, managed_worker_ids)
        desired_per_capset: List[Tuple[Dict[str, int], int]] = [({}, desired)]

        print(
            f"[VANILLA-SCALING][get_scaling_commands]   => desired={desired}, "
            f"sending setDesiredTaskConcurrency with 1 capset (empty caps -> wildcard)"
        )
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _compute_desired_worker_count(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
    ) -> int:
        """Compute the target worker count for this manager from current task and worker observations."""
        current = len(managed_worker_ids)
        task_count = len(information_snapshot.tasks)
        worker_count = len(information_snapshot.workers)
        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency

        print(
            f"[VANILLA-SCALING][_compute_desired] "
            f"current_managed={current} task_count={task_count} "
            f"worker_count={worker_count} max_concurrency={max_concurrency}"
        )

        if worker_count == 0:
            desired_before_cap = current + 1 if task_count > 0 else current
            print(
                f"[VANILLA-SCALING][_compute_desired]   branch=no_workers "
                f"task_count={task_count} => desired_before_cap={desired_before_cap}"
            )
        else:
            task_ratio = task_count / worker_count
            print(
                f"[VANILLA-SCALING][_compute_desired]   task_ratio={task_ratio:.4f} "
                f"(task_count={task_count} / worker_count={worker_count}) "
                f"lower_ratio={self._lower_task_ratio} upper_ratio={self._upper_task_ratio}"
            )
            if task_ratio > self._upper_task_ratio:
                desired_before_cap = current + 1
                print(
                    f"[VANILLA-SCALING][_compute_desired]   branch=above_upper_ratio "
                    f"ratio={task_ratio:.4f} > {self._upper_task_ratio} => desired_before_cap={desired_before_cap} (scale UP)"
                )
            elif task_ratio < self._lower_task_ratio:
                desired_before_cap = 0 if task_count == 0 else max(1, ceil(task_count / self._upper_task_ratio))
                print(
                    f"[VANILLA-SCALING][_compute_desired]   branch=below_lower_ratio "
                    f"ratio={task_ratio:.4f} < {self._lower_task_ratio} task_count={task_count} => desired_before_cap={desired_before_cap} (scale DOWN)"
                )
            else:
                desired_before_cap = current
                print(
                    f"[VANILLA-SCALING][_compute_desired]   branch=within_range "
                    f"ratio={task_ratio:.4f} in [{self._lower_task_ratio}, {self._upper_task_ratio}] => desired_before_cap={desired_before_cap} (no change)"
                )

        desired = desired_before_cap
        if max_concurrency != -1:
            desired = min(desired, max_concurrency)
            if desired != desired_before_cap:
                print(
                    f"[VANILLA-SCALING][_compute_desired]   capped by max_concurrency={max_concurrency}: "
                    f"{desired_before_cap} -> {desired}"
                )

        final = max(0, desired)
        print(
            f"[VANILLA-SCALING][_compute_desired]   FINAL desired={final} "
            f"(pre-floor={desired}, current_managed={current}, delta={final - current:+d})"
        )
        return final
