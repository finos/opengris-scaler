# OCI Integration for OpenGRIS Scaler

OpenGRIS Scaler supports running distributed tasks on Oracle Cloud Infrastructure (OCI)
using Container Instances. Two adapter modes are available depending on the workload
pattern.

## Adapter Modes

### OCI Raw — Persistent Worker Clusters

Launches long-running Container Instances that each host a `scaler_cluster` process
with multiple workers. Best for sustained, steady-state workloads where cold-start
latency is unacceptable.

- Scheduler sends `StartWorkerGroup` / `ShutdownWorkerGroup` commands
- Each Container Instance spawns `instance_ocpus` workers connected back to the scheduler
- Workers execute tasks directly via the Scaler protocol (no Object Storage intermediary)
- Entry point: `scaler_worker_manager_oci_raw_container_instance`

### OCI HPC — Ephemeral Per-Task Containers

Creates a new Container Instance for every task submission. Best for bursty,
compute-heavy workloads where per-task isolation or large fan-out is needed.

- Task payloads are delivered inline (env var) for small inputs or via Object Storage
  for larger ones
- A lightweight job runner script executes the task inside the container and writes
  the result to Object Storage
- The adapter polls the Container Instance lifecycle and fetches results on completion
- Instantiated programmatically via `OCIContainerInstanceWorker`

| Aspect | Raw | HPC |
|---|---|---|
| Worker model | Persistent cluster per instance | One-shot container per task |
| Latency | Low (workers pre-started) | Higher (1-3 min OCI cold start) |
| Scaling | Fixed workers per instance | Dynamic via concurrency semaphore |
| Storage use | None | Object Storage for payloads and results |
| Best for | Steady throughput | Bursty, isolated tasks |

## Prerequisites

- Python 3.8+
- OCI Python SDK: `pip install oci`
- An OCI tenancy with:
  - A Compartment
  - A VCN with at least one Subnet
  - An Availability Domain

## Authentication

Both adapters support two authentication modes, selected by `--oci-auth-type`:

| Mode | Flag value | When to use |
|---|---|---|
| Config file | `config_file` (default) | Local development; reads `~/.oci/config` |
| Instance principal | `instance_principal` | Running on an OCI VM or Container Instance |

The HPC job runner running *inside* a Container Instance authenticates via OCI
Resource Principals, which are automatically available when the instance belongs
to a Dynamic Group with the required IAM policies.

## Quick Start — OCI HPC

### 1. Provision infrastructure

The provisioner creates an Object Storage bucket, Dynamic Group, IAM policy, and
OCIR repository:

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner \
    provision \
    --compartment-id ocid1.compartment.oc1..aaa... \
    --region us-ashburn-1 \
    --availability-domain AD-1 \
    --subnet-id ocid1.subnet.oc1.iad.aaa...
```

This writes a config file (`.scaler_oci_config.json`) used by subsequent commands.

### 2. Build and push the container image

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner \
    build-and-push \
    --config .scaler_oci_config.json
```

The image is built from `Dockerfile.container_instance` and pushed to OCIR.

### 3. Run the test harness

Validate connectivity, Object Storage, and Container Instance lifecycle:

```bash
python tests/worker_manager_adapter/oci_hpc/oci_hpc_test_harness.py \
    --config .scaler_oci_config.json
```

To include end-to-end scheduler task tests, add `--scheduler tcp://<host>:<port>`.

### 4. Clean up

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner \
    cleanup \
    --config .scaler_oci_config.json
```

## Quick Start — OCI Raw

### 1. Build and push the container image

Build the raw adapter image from
`src/scaler/worker_manager_adapter/oci_raw/utility/Dockerfile.container_instance`
and push it to OCIR (or any container registry accessible from your subnet).

### 2. Start the worker adapter

```bash
scaler_worker_manager_oci_raw_container_instance \
    --oci-auth-type config_file \
    --oci-region us-ashburn-1 \
    --compartment-id ocid1.compartment.oc1..aaa... \
    --availability-domain AD-1 \
    --subnet-id ocid1.subnet.oc1.iad.aaa... \
    --container-image <region>.ocir.io/<namespace>/<repo>:latest \
    --scheduler-address tcp://<scheduler_host>:2345
```

The adapter connects to the scheduler and responds to `StartWorkerGroup` /
`ShutdownWorkerGroup` commands by launching or deleting Container Instances.

## Configuration Reference

### OCI Raw (`OCIRawWorkerAdapterConfig`)

| Parameter | Env var | Default | Description |
|---|---|---|---|
| `--oci-auth-type` | `OCI_AUTH_TYPE` | `config_file` | `config_file` or `instance_principal` |
| `--oci-config-profile` | `OCI_CONFIG_PROFILE` | `DEFAULT` | OCI config file profile (config_file mode only) |
| `--oci-region` | `OCI_REGION` | `us-ashburn-1` | OCI region |
| `--compartment-id` | `OCI_COMPARTMENT_ID` | *required* | Compartment OCID |
| `--availability-domain` | `OCI_AVAILABILITY_DOMAIN` | *required* | Availability Domain |
| `--subnet-id` | `OCI_SUBNET_ID` | *required* | Subnet OCID |
| `--container-image` | `OCI_CONTAINER_IMAGE` | *required* | OCIR image URI |
| `--instance-shape` | `OCI_INSTANCE_SHAPE` | `CI.Standard.E4.Flex` | Container Instance shape |
| `--instance-ocpus` | `OCI_INSTANCE_OCPUS` | `4.0` | OCPUs per instance |
| `--instance-memory-gb` | `OCI_INSTANCE_MEMORY_GB` | `30.0` | Memory in GB per instance |

### OCI HPC (`OCIContainerInstanceWorker`)

Instantiated programmatically. Key parameters:

| Parameter | Default | Description |
|---|---|---|
| `compartment_id` | *required* | Compartment OCID |
| `availability_domain` | *required* | Availability Domain |
| `subnet_id` | *required* | Subnet OCID |
| `container_image` | *required* | OCIR image URI |
| `oci_region` | *required* | OCI region |
| `object_storage_namespace` | *required* | OCI Object Storage tenancy namespace |
| `object_storage_bucket` | *required* | Object Storage bucket name |
| `object_storage_prefix` | `scaler-tasks` | Key prefix for inputs/results |
| `instance_shape` | `CI.Standard.E4.Flex` | Container Instance shape |
| `instance_ocpus` | `1.0` | OCPUs per container |
| `instance_memory_gb` | `6.0` | Memory in GB per container |
| `base_concurrency` | `100` | Max concurrent Container Instances |
| `job_timeout_seconds` | `3600` | Per-task timeout |
| `oci_config_profile` | `DEFAULT` | OCI config file profile |

## Project Structure

```
src/scaler/
├── config/section/
│   └── oci_raw_worker_adapter.py       # Raw adapter config dataclass
├── entry_points/
│   └── worker_manager_oci_raw_container_instance.py
├── worker_manager_adapter/
│   ├── oci_raw/
│   │   ├── container_instance.py       # Raw adapter (persistent workers)
│   │   └── utility/
│   │       ├── Dockerfile.container_instance
│   │       └── entrypoint.sh
│   └── oci_hpc/
│       ├── worker.py                   # HPC worker process (scheduler bridge)
│       ├── task_manager.py             # Task queue, submission, polling
│       ├── heartbeat_manager.py        # Scheduler heartbeats
│       ├── callback.py                 # Task-to-instance mapping
│       └── utility/
│           ├── provisioner.py          # Infrastructure provisioner CLI
│           ├── container_instance_job_runner.py  # Runs inside containers
│           └── Dockerfile.container_instance
tests/
└── worker_manager_adapter/
    └── oci_hpc/
        └── oci_hpc_test_harness.py     # 4-phase validation harness
```

## OCI Service Mapping

For users familiar with the AWS adapter, here is how OCI services map:

| AWS | OCI |
|---|---|
| AWS Batch / ECS Fargate | OCI Container Instances |
| Amazon S3 | OCI Object Storage |
| Amazon ECR | OCI Container Registry (OCIR) |
| IAM Role (Batch jobs) | Dynamic Group + IAM Policy |
| CloudWatch Logs | OCI Logging |
| Compute Environment | N/A (Container Instances are fully on-demand) |
| Job Queue | N/A (managed by adapter semaphore) |

## Test Harness

The test harness (`oci_hpc_test_harness.py`) runs four phases:

1. **OCI Connectivity** — Verifies authentication, compartment, subnet, and availability domain
2. **Object Storage** — Creates a temporary bucket, writes/reads/deletes a test object
3. **Container Instance Lifecycle** — Launches a minimal container, polls to completion, deletes
4. **Scheduler Tasks** — Submits real Scaler tasks through a running scheduler (requires `--scheduler`)

Run individual phases with `--phase connectivity|storage|lifecycle|scheduler`.
