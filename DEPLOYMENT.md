# OpenGRIS Scaler OCI — Deployment Guide

This guide covers end-to-end deployment using the companion Terraform repository to provision OCI infrastructure,
followed by building and running the OpenGRIS Scaler OCI worker adapters.

**Repos referenced:**
- `opengris-scaler-terraform` — provisions OCI infrastructure
- `opengris-scaler-oci` — the Scaler application and OCI worker adapters

---

## 1. Prerequisites

### Tools

| Tool | Purpose | Install |
|------|---------|---------|
| Terraform ≥ 1.3 | Provision OCI infrastructure | https://developer.hashicorp.com/terraform/install |
| OCI CLI | Auth setup, config file | https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm |
| Docker | Build and push container images | https://docs.docker.com/get-docker/ |
| Python 3.8+ | Run Scaler locally or install package | https://python.org |
| `uv` (recommended) | Fast Python env management | `pip install uv` |

### OCI Access

- An OCI tenancy with the ability to create resources in a compartment
- Your user must have permission to manage:
  - VCN, subnets, gateways (networking)
  - Compute and Container Instances
  - Object Storage buckets
  - Container Registry (OCIR)
  - IAM Dynamic Groups and Policies (**home region only**)
  - Logging
- An OCI API signing key pair (`~/.oci/config` configured). Run `oci setup config` if not done.
- An OCI Auth Token for Docker login (OCI Console → Identity → Users → your user → Auth Tokens)

### Collect these values before starting

| Value | Where to find |
|-------|--------------|
| Tenancy OCID | OCI Console → top-right user menu → Tenancy |
| Compartment OCID | OCI Console → Identity → Compartments |
| Region identifier | e.g., `us-ashburn-1`, `us-sanjose-1` |
| SSH public key | `~/.ssh/id_ed25519.pub` (generate with `ssh-keygen` if needed) |
| Object Storage namespace | OCI Console → Object Storage → Bucket → Namespace (also: `oci os ns get`) |

---

## 2. Provision Infrastructure with Terraform

### 2.1 Clone and configure

```bash
git clone https://github.com/<org>/opengris-scaler-terraform
cd opengris-scaler-terraform
```

Copy the example vars file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` — at minimum set:

```hcl
tenancy_ocid     = "ocid1.tenancy.oc1...<your-tenancy-ocid>"
region           = "us-ashburn-1"          # target region for workloads
compartment_ocid = "ocid1.compartment.oc1...<your-compartment-ocid>"
```

**To include a scheduler VM and bastion host** (recommended for production):

```hcl
create_scheduler_instance = true
create_bastion            = true
bastion_ssh_cidr          = "YOUR_IP/32"   # restrict SSH access

ssh_public_key_file  = "~/.ssh/id_ed25519.pub"
ssh_private_key_path = "~/.ssh/id_ed25519"
```

> **Note:** Leave `user_ocid`, `fingerprint`, and `private_key_path` empty to use your `~/.oci/config` file.
> IAM resources (Dynamic Groups, Policies) are always created in the tenancy's **home region**, regardless of
> the `region` variable.

### 2.2 Initialize and apply

```bash
make init     # terraform init
make plan     # preview what will be created
make apply    # create resources (~2–3 minutes)
```

Terraform creates:
- VCN (`10.0.0.0/16`) with private worker subnet (`10.0.1.0/24`)
- NAT Gateway (egress for private containers) + Service Gateway (OCIR/Object Storage access)
- Object Storage bucket with 1-day lifecycle expiry on task objects
- OCIR repository for worker images
- IAM Dynamic Group (Container Instances) + policy (Object Storage read/write, Container Instance manage)
- OCI Logging log group for Container Instance stdout/stderr
- *(Optional)* Scheduler compute VM (Oracle Linux 8, `VM.Standard.E4.Flex`)
- *(Optional)* Bastion host in public subnet

### 2.3 Export the Scaler config

```bash
terraform output -json scaler_config > ../opengris-scaler-oci/.scaler_oci_config.json
```

This JSON file drives all subsequent Scaler commands. Example contents:

```json
{
  "oci_region": "us-ashburn-1",
  "tenancy_id": "ocid1.tenancy.oc1...",
  "compartment_id": "ocid1.compartment.oc1...",
  "prefix": "scaler",
  "object_storage_namespace": "mytenancynamespace",
  "object_storage_bucket": "scaler-mytenancynamespace-us-ashburn-1",
  "object_storage_prefix": "scaler-tasks",
  "container_image": "iad.ocir.io/mytenancynamespace/scaler-worker",
  "availability_domain": "AD-1",
  "subnet_id": "ocid1.subnet.oc1...",
  "instance_shape": "CI.Standard.E4.Flex",
  "instance_ocpus": 4.0,
  "instance_memory_gb": 30.0,
  "dynamic_group_name": "scaler-dg",
  "iam_policy_name": "scaler-policy",
  "scheduler_address": "tcp://10.0.1.240:2345"
}
```

---

## 3. Deploy the Application

### 3.1 Install the Scaler package

In the `opengris-scaler-oci` directory:

```bash
cd ../opengris-scaler-oci

uv venv .venv
source .venv/bin/activate
uv pip install -e ".[oci]"
```

This installs all Scaler CLI entry points and the OCI SDK.

### 3.2 Log in to OCIR

```bash
cd ../opengris-scaler-terraform
make login
# Prompts: Docker username = <object-storage-namespace>/<oci-username>
#          Password = your OCI Auth Token
```

Or manually:

```bash
docker login <region-key>.ocir.io \
  -u <object-storage-namespace>/<oci-username>
# Enter Auth Token as password
```

Region key examples: `iad` (us-ashburn-1), `sjc` (us-sanjose-1), `phx` (us-phoenix-1).

### 3.3 Build and push container images

From the Terraform repo (uses `scripts/build-and-push.sh`):

```bash
# Build both HPC and Raw adapter images
make build SCALER_SRC=../opengris-scaler-oci ADAPTER=both

# Or build a specific adapter only:
make build SCALER_SRC=../opengris-scaler-oci ADAPTER=hpc
make build SCALER_SRC=../opengris-scaler-oci ADAPTER=raw
```

This builds the Dockerfiles in `opengris-scaler-oci/src/scaler/worker_manager_adapter/oci_hpc/utility/` and
`oci_raw/utility/`, tags them with `latest`, and pushes to OCIR.

> **Tip:** To build manually without the Makefile:
> ```bash
> docker build -f src/scaler/worker_manager_adapter/oci_hpc/utility/Dockerfile.container_instance \
>   -t iad.ocir.io/<namespace>/scaler-worker-hpc:latest \
>   src/scaler/worker_manager_adapter/oci_hpc/utility/
> docker push iad.ocir.io/<namespace>/scaler-worker-hpc:latest
> ```

### 3.4 Deploy the scheduler (if using the Terraform-provisioned scheduler VM)

> Skip this section if you are running the scheduler on a machine already reachable by your Container Instances.

The scheduler VM is provisioned by Terraform but starts with an empty `/opt/scaler/src` directory.
Deploy your local source and restart services:

```bash
cd opengris-scaler-terraform
make deploy SCALER_SRC=../opengris-scaler-oci
```

This script:
1. Tarballs the Scaler source (excluding `.git`, `__pycache__`, build artifacts)
2. Copies it to the scheduler VM via SCP (through the bastion if enabled)
3. Creates a Python 3.11 venv at `/opt/scaler/venv`
4. Installs `opengris-scaler[oci]` (compiles C++ extensions with GCC 14)
5. Restarts `scaler-scheduler` and `scaler-worker-manager` systemd services

> **First boot:** The scheduler VM compiles Cap'n Proto, ZeroMQ, and libuv from source on first boot.
> This takes **15–25 minutes**. Monitor progress:
> ```bash
> make ssh
> sudo tail -f /var/log/scaler-init.log
> ```
> Wait until the log shows completion before running `make deploy`.

### 3.5 Run the worker adapter locally (alternative to scheduler VM)

If you are not using the Terraform-provisioned scheduler VM, you can run the worker manager adapter directly
on any machine that can reach your scheduler.

**OCI HPC adapter** (ephemeral Container Instances per task):

```bash
scaler_worker_manager_oci_hpc --config .scaler_oci_config.json
```

**OCI Raw adapter** (persistent Container Instance worker pools):

```bash
scaler_worker_manager_oci_raw_container_instance \
  --config .scaler_oci_config.json \
  --oci-auth-type config_file
```

For both adapters, `--scheduler-address` defaults to the value in `.scaler_oci_config.json`.
Override if needed: `--scheduler-address tcp://<host>:2345`.

---

## 4. Key Configuration Values

### `.scaler_oci_config.json` fields

| Field | Description | Example |
|-------|-------------|---------|
| `oci_region` | OCI region for workloads | `us-ashburn-1` |
| `compartment_id` | Compartment OCID | `ocid1.compartment.oc1...` |
| `object_storage_namespace` | Tenancy Object Storage namespace | `mytenancynamespace` |
| `object_storage_bucket` | Bucket name for task payloads/results | `scaler-mytenancynamespace-us-ashburn-1` |
| `object_storage_prefix` | Key prefix within bucket | `scaler-tasks` |
| `container_image` | OCIR image URI (without tag) | `iad.ocir.io/mytenancynamespace/scaler-worker` |
| `availability_domain` | AD for Container Instances | `AD-1` |
| `subnet_id` | Subnet OCID for Container Instances | `ocid1.subnet.oc1...` |
| `instance_shape` | Container Instance shape | `CI.Standard.E4.Flex` |
| `instance_ocpus` | OCPUs per instance | `4.0` |
| `instance_memory_gb` | Memory per instance | `30.0` |
| `scheduler_address` | ZMQ TCP address of the scheduler | `tcp://10.0.1.240:2345` |

### Terraform variables (key non-defaults in `terraform.tfvars`)

| Variable | Default | When to change |
|----------|---------|----------------|
| `bucket_lifecycle_days` | `1` | Increase if tasks run longer than 1 day |
| `bastion_ssh_cidr` | `0.0.0.0/0` | **Always restrict** to your IP/range in production |
| `instance_ocpus` / `instance_memory_gb` | `4.0` / `30.0` | Tune for your task workload |
| `create_scheduler_instance` | `false` | Set `true` to provision scheduler VM |
| `create_bastion` | `false` | Set `true` for SSH access into the private VCN |
| `ocir_repository_is_public` | `false` | Leave `false` for private images |

### Authentication modes

| Mode | Config | Use when |
|------|--------|---------|
| `config_file` (default) | Reads `~/.oci/config` | Running adapter locally or on non-OCI machine |
| `instance_principal` | Uses OCI IAM instance principal | Running adapter on an OCI VM in the Dynamic Group |

On the Terraform-provisioned scheduler VM, `instance_principal` is used automatically.

---

## 5. Verify a Successful Deployment

### 5.1 Run the OCI HPC test harness

The test harness validates all four integration layers end-to-end:

```bash
cd opengris-scaler-oci
python tests/worker_manager_adapter/oci_hpc/oci_hpc_test_harness.py \
  --config .scaler_oci_config.json \
  --test all
```

This runs four phases:
1. **OCI connectivity** — SDK auth, compartment access, subnet, availability domains
2. **Object Storage** — put/get/delete a test object in the task bucket
3. **Container Instance lifecycle** — create a minimal CI, poll until completion
4. **Scheduler integration** — submit real Scaler tasks (add `--scheduler tcp://<host>:2345`)

All four phases must pass before using in production.

### 5.2 Verify the scheduler VM (if provisioned)

```bash
cd opengris-scaler-terraform
make ssh   # SSH to scheduler via bastion

# On the scheduler VM:
systemctl status scaler-scheduler
systemctl status scaler-worker-manager
journalctl -u scaler-scheduler -n 50
journalctl -u scaler-worker-manager -n 50
```

### 5.3 Submit a test task from a client

```python
import scaler
client = scaler.Client(address="tcp://<scheduler-address>:2345")
result = client.submit(lambda x: x * 2, 21)
assert result == 42
print("Deployment verified!")
```

### 5.4 Spot-check via OCI Console

| Resource | Where to look |
|----------|--------------|
| Container Instances | OCI Console → Container Instances → your compartment |
| Task objects | OCI Console → Object Storage → `scaler-<namespace>-<region>` bucket |
| Container logs | OCI Console → Logging → Log Group `scaler-logs` |
| OCIR images | OCI Console → Container Registry → `scaler-worker` repository |

---

## 6. Teardown / Cleanup

### Remove OCI provisioned resources via Terraform

```bash
cd opengris-scaler-terraform
make destroy
```

This removes all resources created by Terraform: VCN, subnets, gateways, security lists, Object Storage bucket,
OCIR repository, IAM Dynamic Group and Policy, Log Group, and optionally the scheduler and bastion VMs.

> **Warning:** `make destroy` will permanently delete the Object Storage bucket and all task data in it.
> Make sure there are no in-progress tasks before running.

### Cleanup provisioned resources via the Python provisioner (HPC adapter only)

If you provisioned resources using the Python provisioner (without Terraform):

```bash
python -m scaler.worker_manager_adapter.oci_hpc.utility.provisioner \
  cleanup --config .scaler_oci_config.json
```

This removes: Object Storage bucket, Dynamic Group, IAM Policy, and OCIR repository.

### Remove local files

```bash
rm .scaler_oci_config.json
```

---

## Appendix: Quick Reference

### Common commands

```bash
# Show all Terraform outputs
cd opengris-scaler-terraform && terraform output

# Re-export Scaler config after Terraform changes
terraform output -json scaler_config > ../opengris-scaler-oci/.scaler_oci_config.json

# SSH to scheduler (requires bastion)
make ssh

# Build HPC image only
make build SCALER_SRC=../opengris-scaler-oci ADAPTER=hpc

# Re-deploy source to scheduler after code changes
make deploy SCALER_SRC=../opengris-scaler-oci

# Check scheduler service logs
make ssh -- journalctl -u scaler-scheduler -f
```

### Adapter quick comparison

| | OCI HPC | OCI Raw |
|--|---------|---------|
| **Container lifecycle** | One CI per task (ephemeral) | Persistent CI pool |
| **Cold start** | ~30s per task | ~5 min initial, then amortized |
| **Best for** | Bursty, isolated, long-running tasks | Steady-state, many lightweight tasks |
| **Concurrency limit** | 100 concurrent CIs (configurable) | Workers per instance × instances |
| **Task timeout** | `job_timeout_seconds` (default: 3600s) | No per-task timeout |
| **Container image** | `Dockerfile.container_instance` (oci_hpc) | `Dockerfile.container_instance` (oci_raw) |
| **Entry point** | `scaler_worker_manager_oci_hpc` | `scaler_worker_manager_oci_raw_container_instance` |
