# OpenGRIS Scaler — OCI Integration Guide

OpenGRIS Scaler enables distributed task execution on Oracle Cloud Infrastructure through two adapter modes optimized for different workload patterns.

---

## Two Adapter Modes

**OCI Raw (Persistent Workers):**
Launches long-running Container Instances that each host a `scaler_cluster` process with multiple workers. Best for steady-state workloads where startup latency should be amortized across many tasks.

**OCI HPC (Ephemeral Containers):**
Creates a new Container Instance for every task submission. Task data transfers via Object Storage; a job runner executes the task and returns the result. Best for bursty, isolated, or long-running workloads where per-task isolation matters.

---

## Prerequisites

- Python 3.8+
- OCI Python SDK: `pip install opengris-scaler[oci]`
- An OCI tenancy with: Compartment, VCN with Subnet, Availability Domain, Object Storage namespace

> **Starting from scratch?** Use the companion Terraform repo to provision all required infrastructure automatically before following the steps below:
> [https://github.com/oci-fsi-pursuits/scaler-oci-terraform](https://github.com/oci-fsi-pursuits/scaler-oci-terraform)
>
> If you use Terraform, export its outputs as your config file and jump straight to Step 2:
> ```bash
> cd scaler-oci-terraform
> terraform output -json scaler_config > .scaler_oci_config.json
> # Then come back here and start at Step 2
> ```
>
> If you already have OCI infrastructure set up, proceed with manual prerequisites below.

### Manual Prerequisites

Gather the following values — you'll need them in every step below:

| Value | Where to find it |
|-------|-----------------|
| `compartment_id` | OCI Console → Identity → Compartments |
| `subnet_id` | OCI Console → Networking → Virtual Cloud Networks → your VCN → Subnets |
| `availability_domain` | OCI Console → top-right region picker, e.g. `AD-1` |
| `region` | Your OCI region identifier, e.g. `us-ashburn-1` |
| `object_storage_namespace` | OCI Console → Object Storage → Namespace (top of bucket list page) |

---

## Authentication

Two modes are available, selectable via `--oci-auth-type`:

- **`config_file`** (default): Reads `~/.oci/config`. Use for local development.
- **`instance_principal`**: Uses OCI Instance Principal credentials. Use when running on an OCI VM or Container Instance that belongs to a Dynamic Group with the appropriate IAM policies. **This is the correct mode when running on a scheduler VM provisioned by the Terraform repo.**

Container job runners (OCI HPC mode) authenticate automatically via OCI Resource Principals — no configuration needed inside the container.

---

## OCI HPC Quick Start

### Step 1 — Provision Infrastructure

Skip this step if you used the Terraform repo (your `.scaler_oci_config.json` is already generated).

Run the provisioner to create the Object Storage bucket, Dynamic Group, IAM policies, and OCIR repository:

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner provision \
  --compartment-id <OCID> \
  --region us-ashburn-1 \
  --availability-domain AD-1 \
  --subnet-id <OCID>
```

This writes a `.scaler_oci_config.json` file in the current directory. Keep it — all subsequent steps read from it.

### Step 2 — Build and Push the Container Image

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner \
  build-image --config .scaler_oci_config.json
```

This builds the job runner image and pushes it to your OCIR repository. The image only needs `cloudpickle` and `oci` — it does not require the full scaler package.

### Step 3 — Run the Test Harness

Validate all four phases before running real workloads:

```bash
python tests/worker_manager_adapter/oci_hpc/oci_hpc_test_harness.py \
  --config .scaler_oci_config.json
```

The harness checks OCI connectivity, Object Storage operations, Container Instance lifecycle, and (optionally) scheduler task integration in sequence. All four phases must pass before proceeding.

### Step 4 — Start the Worker Manager

```bash
scaler_worker_manager_oci_hpc \
  --config .scaler_oci_config.json \
  --scheduler-address tcp://<scheduler-host>:2345 \
  --oci-auth-type config_file
```

Use `--oci-auth-type instance_principal` instead if running on an OCI VM.

### Step 5 — Clean Up (when done)

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner \
  cleanup --config .scaler_oci_config.json
```

---

## OCI Raw Quick Start

### Step 1 — Build and Push the Worker Image

Build from `Dockerfile.container_instance` and push to your OCIR repository:

```bash
# Authenticate with OCIR first
docker login <region>.ocir.io -u <tenancy-namespace>/<username>

# Build and push
docker build -f Dockerfile.container_instance \
  -t <region>.ocir.io/<namespace>/<repo>:latest .
docker push <region>.ocir.io/<namespace>/<repo>:latest
```

If you used the Terraform repo, `make login && make build ADAPTER=raw` handles this.

### Step 2 — Start the Worker Manager

```bash
scaler_worker_manager_oci_raw_container_instance \
  --oci-auth-type config_file \
  --oci-region us-ashburn-1 \
  --compartment-id <OCID> \
  --availability-domain AD-1 \
  --subnet-id <OCID> \
  --container-image <region>.ocir.io/<namespace>/<repo>:latest \
  --scheduler-address tcp://<scheduler-host>:2345
```

> **Important:** The scheduler address must be reachable from inside OCI Container Instances on your subnet. If your scheduler is running locally, it will not be reachable. Use a scheduler running on an OCI VM (see the Terraform repo's optional scheduler instance), or configure port forwarding through a bastion host.

---

## Key Configuration Parameters

### OCI Raw

| Parameter | Default | Notes |
|-----------|---------|-------|
| `--instance-shape` | `CI.Standard.E4.Flex` | |
| `--instance-ocpus` | `4.0` | |
| `--instance-memory-gb` | `30.0` | |
| `--oci-auth-type` | `config_file` | Use `instance_principal` on OCI VMs |

### OCI HPC (via `.scaler_oci_config.json`)

| Parameter | Default | Notes |
|-----------|---------|-------|
| `base_concurrency` | `100` | Max concurrent Container Instances |
| `job_timeout_seconds` | `3600` | Per-task limit (1 hour) |
| `instance_ocpus` | `1.0` | |
| `instance_memory_gb` | `6.0` | |

> **Object Storage lifecycle note:** The Terraform repo sets a 7-day object expiry on the task bucket by default. If you provision manually, ensure your bucket lifecycle is at least as long as your longest expected `job_timeout_seconds` to avoid silent result-fetch failures.

---

## OCI vs AWS Service Mapping

| AWS | OCI |
|-----|-----|
| AWS Batch / ECS Fargate | Container Instances |
| Amazon S3 | Object Storage |
| Amazon ECR | OCIR |
| IAM Role | Dynamic Group + Policy |

---

## Test Harness Phases

The validation harness runs four sequential checks:

1. **OCI Connectivity** — verifies SDK auth and compartment access
2. **Object Storage** — put/get/delete a test object in the bucket
3. **Container Instance lifecycle** — launches a test container, waits for INACTIVE, verifies exit
4. **Scheduler integration** *(optional)* — submits a real task through a running scheduler

All four phases should pass before using the adapters in production.

---

## Troubleshooting

**Worker manager fails to authenticate on first startup**
- If running locally: ensure `~/.oci/config` exists and the profile has correct tenancy/user OCIDs, fingerprint, and key path. Use `oci iam user list` to verify.
- If running on an OCI VM: use `--oci-auth-type instance_principal` and ensure the VM's OCID is in the Dynamic Group policy.

**Container Instances created but result never returns**
- Check the Object Storage bucket lifecycle — if set to 1 day, results from long-running tasks may expire before the adapter fetches them. The recommended minimum is 7 days.
- Check the container logs in OCI Console → Container Instances → your instance → Logs.

**`scaler_cluster` not found in container (OCI Raw)**
- Ensure `SCALER_PACKAGE` is set in the container environment, e.g. `SCALER_PACKAGE=opengris-scaler[oci]`. The entrypoint installs it at startup.

**Scheduler not reachable from containers**
- Container Instances run inside OCI's network. A scheduler on your laptop is not reachable. Deploy the scheduler to an OCI VM (the Terraform repo provides an optional scheduler instance with systemd services pre-configured).
