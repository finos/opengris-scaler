# End-to-end integration tests

Full-stack tests of OpenGRIS Scaler's scaling control loop. Every worker manager (ECS, ORB/EC2, Batch,
native, container, ...) is a `DeclarativeWorkerProvisioner` driven by the **same** scheduler command
(`setDesiredTaskConcurrency`) over the **same** wire protocol, so the suite splits by *how much is real*:

| Test | File(s) | What is real | What is mocked / simulated | Gate |
|------|---------|--------------|----------------------------|------|
| **AWS control plane** | `test_ecs_provisioning.py`, `test_ec2_orb_provisioning.py`, `test_batch_provisioning.py` | the real provisioner, the real boto3 request path, the real scheduler command object | AWS itself (moto, in-process). **Nothing boots and no task runs.** | `RUN_INTEGRATION_TESTS=1` |
| **Container-scaling e2e** | `test_container_scaling_e2e.py` | a real client, scheduler, and **real workers in real Docker containers** (each its own IP) that run real tasks | the "cloud": a container stands in for a provisioned instance (no AWS at all) | `RUN_CONTAINER_E2E=1` (+ Docker) |
| **ECS scaling e2e** | `test_ecs_scaling_e2e.py` | the **shipped ECS worker manager** and its boto3 calls, **plus real workers in real ECS task containers** | AWS is a local [floci](https://github.com/hechmik/floci) emulator that actually launches `RunTask` containers | `RUN_FLOCI_E2E=1` (+ Docker) |

The AWS control-plane tests ask *"does the manager drive AWS correctly?"*; the two Docker e2es ask *"do
real workers provision, connect, scale, and return correct results?"* -- the container e2e with no cloud at
all, the ECS e2e through the *shipped* ECS manager against an emulator that boots the containers for real.

Every test is **opt-in** behind an environment gate and skips itself otherwise, so the default
`python -m unittest discover` build never pays their cost.

## AWS control plane (moto)

The provisioner's boto3 calls are asserted against **moto**, an in-process boto3 mock -- fast, free,
cross-platform, no Docker. It reimplements AWS below botocore and covers all three managers' control planes
(ECS, EC2, Batch + ECR). It does **not** boot the compute it "provisions", so these tests cover only the
control plane; the small gaps between moto and real AWS are centralised in `_aws_backend.py` (see notes
below) so the provisioner code runs unchanged.

## Container-scaling e2e (Docker)

`test_container_scaling_e2e.py` runs the whole loop with **no cloud and no AWS mock**: a real client
bursts work at a real scheduler, whose scaling policy drives one or more `ContainerWorkerProvisioner`s
(in `_container_backend.py`) to launch container "machines" -- each a *fixed* `baremetal_native` worker
manager with its own IP -- which run the tasks and return real results. Correct results are the proof
that real containerized workers ran the work; the running-container count is the proof of how the pool
scaled. Two classes:

* **`TestContainerScalingE2E`** -- one auto-scaling manager against a scheduler that starts with zero
  machines: a burst scales the pool **up** across container machines and returns correct results; an idle
  queue drains it back to **zero**; a deep burst **spreads** work across several machines; and a
  concurrency-1 trickle followed by a burst provisions a machine **mid-flight** under rising load.
* **`TestContainerWaterfallE2E`** -- two container managers at different **waterfall priorities** on one
  scheduler: work fills the high-priority manager first and **spills** onto the low-priority one when it
  saturates.

This is the free, local analog of a cloud manager provisioning an instance whose user-data starts a
worker. The provisioner is built for it: per-manager `worker_manager_id` / container prefix,
`workers_per_machine` / `max_machines`, and `boot_delay_seconds` / `shutdown_delay_seconds` knobs to
simulate cloud latency.

## ECS scaling e2e (floci)

`test_ecs_scaling_e2e.py` drives the **shipped `ECSWorkerManager`** (`scaler.worker_manager_adapter.aws_raw.ecs`)
-- nothing about the worker manager is faked -- against [floci](https://github.com/hechmik/floci), a free
local AWS emulator that, unlike moto or community LocalStack, actually launches each ECS `RunTask` as a
sibling Docker container (through the host docker socket). So the exact production ECS code path runs, boto3
is merely pointed at floci via `AWS_ENDPOINT_URL`, and real workers connect back, run work, and return
results. It closes the gap the control-plane ECS test leaves open (which never boots anything).

* The scheduler's scaling policy drives the manager to launch ECS task containers; correct results prove
  those containers ran the work and the running-container count proves how the pool scaled up and drained.
* Tasks are submitted **by value** (nested functions, cloudpickled whole) because the shipped provisioner
  mounts no repo into the task, and each task tags its work by **container hostname** (the provisioner sets
  no machine id), so a test can see work **spread** across tasks.
* floci (`_floci.py`) is started per test with the docker socket mounted; the emulator and the entrypoint
  image (`ecs.Dockerfile`, built on the container-scaling `worker.Dockerfile`) are the only new pieces --
  everything downstream is the shipped manager.

**Watching either Docker e2e run.** The harness starts the web GUI wired to the scheduler monitor and
prints a `web GUI: http://localhost:PORT` line; open it during a local run to watch the pool scale. (It is
on in CI too, just unwatched, to keep the setup identical.)

## The self-contained worker image (no host-layout coupling)

Both Docker e2es run their workers from a small image (`worker.Dockerfile`, built by `_container_image.py`)
that installs the host's freshly built wheel plus the custom capnp/kj runtime libs, so a container is
byte-identical to the scheduler it talks to -- the wire protocol always matches. The container e2e mounts
the repo read-only so a worker can import its task module; the ECS e2e needs no mount (tasks travel by
value) and layers a thin `COMMAND`-exec entrypoint on top (`ecs.Dockerfile`). The container runtime is
abstracted (`_container_runtime.py`, Docker today, `SCALER_IT_CONTAINER_CLI`-swappable for podman), and the
workers reach the host scheduler + object storage over the docker-bridge gateway
(`SchedulerHarness(gateway=...)` binds `0.0.0.0` and advertises the gateway address). Both need a Docker
daemon, so each has its own gate and neither runs in the standard CI lanes.

## Where is real task execution covered?

moto does not boot the compute it "provisions", so the AWS control-plane tests cannot verify task
execution. That is covered by the two **Docker e2es** (in containers) plus `tests/scheduler/test_scaling.py`
in the **default suite** (a scheduler-from-zero + native manager asserting dynamically provisioned worker
*processes* return correct results, on every push). The floci ECS e2e is the closest to a real cloud data
plane -- the shipped manager launching real task containers -- without a paid tier or a real AWS account.

## Running

```bash
# Install test dependencies (moto is in-process; the AWS control-plane tests need no Docker):
uv pip install -e '.[all]' --group dev

# AWS control plane (moto):
RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v

# Container-scaling e2e (Docker): builds the worker image, brings up the stack + web GUI, runs the scenarios:
./scripts/run_container_e2e.sh                   # DOCKER="sudo docker" ./... if the socket is root-only

# ECS scaling e2e (floci + Docker): builds the ECS task image, starts a floci emulator, drives the shipped
# ECS manager through the scale curve:
./scripts/run_floci_e2e.sh                       # DOCKER="sudo docker" ./... if the socket is root-only
```

**In CI:** the AWS control-plane tests run on the Linux lane with moto (see
`.github/actions/run-test/action.yml`). The two Docker e2es run on demand: the **`Container Scaling E2E`**
workflow (`container-e2e.yml`) and the **`floci ECS Scaling E2E`** workflow (`floci-e2e.yml`) -- trigger
either from the Actions tab, or add the **`container-e2e`** / **`floci-e2e`** label to a PR.

## moto compatibility notes

`_aws_backend.py` centralises the small gaps between moto and real AWS so the provisioner runs unchanged:

1. `MOTO_IAM_LOAD_MANAGED_POLICIES=true` -- so the provisioner can attach AWS-managed policies (e.g.
   `AmazonECSTaskExecutionRolePolicy`).
2. The ECS environment (VPC with DNS hostnames, subnet, security group, cluster, and a task definition
   with container-level memory) is pre-seeded, as a real account already would be. This also lets the
   provisioner *discover* the task definition rather than register one (which trips a moto Fargate
   memory-summing quirk).
3. A `before-parameter-build.ecs.RunTask` handler injects a default `securityGroups` (moto requires it;
   real AWS defaults it), and a matching Batch handler injects the `serviceRole`
   `CreateComputeEnvironment` needs.
