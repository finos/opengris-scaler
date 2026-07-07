# End-to-end integration tests

Full-stack tests of OpenGRIS Scaler's scaling control loop. Every worker manager (ECS, ORB/EC2, Batch,
native, ...) is a `DeclarativeWorkerProvisioner` driven by the **same** scheduler command
(`setDesiredTaskConcurrency`) over the **same** wire protocol, so the suite splits the loop into three
layers by *what is real*, and each layer can be exercised on its own.

Every test is **opt-in** behind an environment gate and skips itself otherwise, so the default
`python -m unittest discover` build never pays their cost.

## The three layers

| Layer | File(s) | What is real | What is mocked / simulated | Gate |
|-------|---------|--------------|----------------------------|------|
| **1. AWS control plane** | `test_ecs_provisioning.py`, `test_ec2_orb_provisioning.py`, `test_batch_provisioning.py` | the real provisioner, the real boto3 request path, the real scheduler command object | AWS itself (moto in-process, or LocalStack over real HTTP). **Nothing boots and no task runs.** | `RUN_INTEGRATION_TESTS=1` |
| **2. Container-scaling data plane** | `test_container_scaling_e2e.py` | a real client, scheduler, and **real workers in real Docker containers** (each its own IP) that run real tasks; scheduler-driven scale-up | the "cloud": a container stands in for an instance, with optional boot/shutdown delays to simulate provisioning latency | `RUN_CONTAINER_E2E=1` (+ Docker) |
| **3. Process scaling** | `test_process_scaling_e2e.py` | a real client, scheduler, and many real worker **processes**; scale up / down / to-zero and multi-"machine" spread | nothing -- all local processes | `RUN_PROCESS_SCALING_TEST=1` |

Layer 1 asks *"does the manager drive AWS correctly?"*; layers 2 and 3 ask *"do real workers provision,
connect, and return correct results under scaling?"* -- layer 2 across container boundaries (distinct
IPs, the base for nested-client and multi-machine scenarios), layer 3 at process scale on one box.

## Layer 1 -- AWS control plane

The provisioner's boto3 calls are asserted against a mocked AWS; no compute is launched.

**Which backend covers which manager:**

| worker manager | AWS service | moto | free community LocalStack | LocalStack Pro |
|----------------|-------------|:----:|:-------------------------:|:--------------:|
| ECS (`aws_raw`) | ECS | yes | no (ECS is Pro-only) | yes |
| EC2 (`orb_aws_ec2`) | EC2 | yes | yes (**EC2 is free-tier**) | yes |
| Batch (`aws_hpc`) | Batch + ECR | yes | no (Batch/ECR are Pro-only) | yes |

* **moto** (default, used in CI) reimplements AWS *in-process* and intercepts boto3 below botocore --
  fast, free, cross-platform, no Docker. It implements ECS, so it runs the whole ECS control-plane test.
  The small gaps between moto and real AWS are centralised in `_aws_backend.py` (see notes below) so the
  provisioner code runs unchanged.
* **LocalStack** is a **pure AWS API mock** that boto3 talks to over real HTTP: it adds a
  higher-fidelity cross-check of the serialization/endpoint/region path that moto's in-process
  interception bypasses. It does **not** boot instances or run tasks -- that is layer 2's job. Community
  LocalStack does not implement ECS or Batch (Pro features), so those tests skip themselves against it;
  set a `LOCALSTACK_AUTH_TOKEN` to run them against Pro.

## Layer 2 -- Container-scaling data plane

`test_container_scaling_e2e.py` runs the whole loop with **no cloud and no AWS mock**: a real client bursts
work at a real scheduler, whose scaling policy drives a `ContainerWorkerProvisioner` (in
`_container_backend.py`) to launch container "machines" -- each a *fixed* `baremetal_native` worker
manager with its own IP -- which run the tasks and return real results. Correct results are themselves
the proof that real containerized workers ran the work.

This is the free, local analog of a cloud manager provisioning an instance whose user-data starts a
worker, and it is the **base for richer e2e scenarios** (scale up/down under load, multi-machine spread,
nested clients with distinct IPs) that no in-process or cloud-mock test can reach.

**How a container runs the host's scaler (no image build).** Rather than build and version-match an
image, each container (`ubuntu:26.04`, chosen to match the host glibc) runs the host's already-built
scaler via read-only bind-mounts -- the repo, the uv interpreter store, and the C++ runtime libs (see
`_container_backend.scaler_bind_mounts`). It is therefore byte-identical to the scheduler it talks to, so
the wire protocol always matches. The container runtime is abstracted (`_container_runtime.py`, Docker
today, `SCALER_IT_CONTAINER_CLI`-swappable for podman), and the workers reach the host scheduler + object
storage over the docker-bridge gateway (`SchedulerHarness(gateway=...)` binds `0.0.0.0` and advertises
the gateway address). Because it depends on the host's dev layout + a Docker daemon, it has its own gate
(`RUN_CONTAINER_E2E=1`) and never runs in the standard CI lanes.

## Layer 3 -- Process scaling (`test_process_scaling_e2e.py`)

Heavier, opt-in tests that simulate a small distributed system on one machine with **local worker
processes** (no containers). Two shapes:

* **`TestProcessScalingE2E`** -- one dynamic manager against a scheduler that starts with **zero**
  workers: a burst of light tasks scales the pool **up** across several real worker processes; a steady
  light load scales it back **down**; an idle queue drains it to **zero**.
* **`TestMultiMachineScalingE2E`** -- multiple managers, one per simulated **"machine"**, each
  provisioning its own workers on one scheduler; asserts work spreads across machines and that
  provisioning a new machine mid-flight adds capacity. Each manager tags its workers via an env var so a
  task reports which machine ran it.

Both are tuned by env vars so they can be scaled up on a bigger box without code changes:

```bash
RUN_PROCESS_SCALING_TEST=1 \
  PROCESS_SCALING_MAX_WORKERS=8 PROCESS_SCALING_TASKS=240 PROCESS_SCALING_TASK_SECONDS=0.2 \
  PROCESS_SCALING_MIN_WORKERS=3 PROCESS_SCALING_MACHINES=3 PROCESS_SCALING_WORKERS_PER_MACHINE=2 \
  python -m unittest tests.integration.test_process_scaling_e2e -v
```

## Where is real task execution covered?

Neither moto nor community LocalStack boots the compute they "provision", so layer 1 cannot verify task
execution. That is covered by **layer 2** (`test_container_scaling_e2e.py`, in containers) and **layer 3**
(`test_process_scaling_e2e.py`, in processes), plus `tests/scheduler/test_scaling.py` in the default
suite (a scheduler-from-zero + native manager asserting dynamically provisioned workers return correct
results). A real *cloud* data plane -- provisioned instances that actually boot and connect back --
needs LocalStack Pro (its ECS/EC2 Docker backend) or a comparable real backend, and is out of scope here.

## Running

```bash
# Install test dependencies (moto is in-process; layer 1 needs no Docker):
uv pip install -e '.[all]' --group dev

# Layer 1 -- AWS control plane (moto backend, the CI default):
RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v

# Layer 1 against a real LocalStack instead of moto (starts a container, runs, tears down):
./scripts/run_integration_localstack.sh          # DOCKER="sudo docker" ./... if the socket is root-only

# Layer 2 -- container-scaling e2e (needs a Docker daemon):
RUN_CONTAINER_E2E=1 python -m unittest tests.integration.test_container_scaling_e2e -v

# Layer 3 -- process scaling:
RUN_PROCESS_SCALING_TEST=1 python -m unittest tests.integration.test_process_scaling_e2e -v
```

**In CI:** layer 1 runs on the Linux lane with moto (see `.github/actions/run-test/action.yml`).
LocalStack runs on demand via the **`LocalStack AWS-Mock Cross-Check`** workflow
(`integration-localstack.yml`): trigger it from the Actions tab or add the **`localstack`** label to a
PR. The process scaling test runs on demand via the **`Process Scaling Test`** workflow. The container
e2e (`RUN_CONTAINER_E2E`) is developer/manual-run: it depends on a Docker daemon and the host's dev
layout, so it is not wired into a standard CI lane.

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
