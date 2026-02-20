from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.worker_adapter.drivers.common import format_capabilities


def get_orb_worker_name(instance_id: str) -> str:
    """
    Returns the deterministic worker name for an ORB instance.
    If instance_id is the bash variable '${INSTANCE_ID}', it returns a bash-compatible string.
    """
    if instance_id == "${INSTANCE_ID}":
        return "Worker|ORB|${INSTANCE_ID}|${INSTANCE_ID//i-/}"
    tag = instance_id.replace("i-", "")
    return f"Worker|ORB|{instance_id}|{tag}"


def create_user_data(
    worker_config: WorkerConfig, adapter_config: WorkerAdapterConfig, event_loop: str, worker_io_threads: int
) -> str:
    # We assume 1 worker per machine for ORB
    # TODO: Add support for multiple workers per machine if needed
    num_workers = 1

    # Build the command
    # We construct the full WorkerID here so it's deterministic and matches what the adapter calculates
    # We fetch instance_id once and use it to construct the ID
    script = f"""#!/bin/bash
INSTANCE_ID=$(ec2-metadata --instance-id --quiet)
WORKER_NAME="{get_orb_worker_name('${INSTANCE_ID}')}"

nohup /usr/local/bin/scaler_cluster {adapter_config.scheduler_address.to_address()} \
    --num-of-workers {num_workers} \
    --worker-names "${{WORKER_NAME}}" \
    --per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \
    --heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \
    --task-timeout-seconds {worker_config.task_timeout_seconds} \
    --garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \
    --death-timeout-seconds {worker_config.death_timeout_seconds} \
    --trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \
    --event-loop {event_loop} \
    --worker-io-threads {worker_io_threads} \
    --deterministic-worker-ids"""

    if worker_config.hard_processor_suspend:
        script += " \
    --hard-processor-suspend"

    if adapter_config.object_storage_address:
        script += f" \
    --object-storage-address {adapter_config.object_storage_address.to_string()}"

    capabilities = worker_config.per_worker_capabilities.capabilities
    if capabilities:
        cap_str = format_capabilities(capabilities)
        if cap_str.strip():
            script += f" \
    --per-worker-capabilities {cap_str}"

    script += " > /var/log/opengris-scaler.log 2>&1 &"

    return script
