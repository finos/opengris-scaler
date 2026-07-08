# End-to-end integration tests

Full-stack tests of OpenGRIS Scaler's scaling control loop. Every worker manager (ECS, ORB/EC2, Batch,
native, container, ...) is a `DeclarativeWorkerProvisioner` driven by the **same** scheduler command
(`setDesiredTaskConcurrency`) over the **same** wire protocol, so the suite has two families, split by
*what is real*:

| Family | File(s) | What is real | What is mocked / simulated | Gate |
|--------|---------|--------------|----------------------------|------|
| **AWS control plane** | `test_ecs_provisioning.py`, `test_ec2_orb_provisioning.py`, `test_batch_provisioning.py` | the real provisioner, the real boto3 request path, the real scheduler command object | AWS itself (moto in-process, or LocalStack over real HTTP). **Nothing boots and no task runs.** | `RUN_INTEGRATION_TESTS=1` |
| **Full e2e (Docker)** | `test_container_scaling_e2e.py` | a real client, scheduler, and **real workers in real Docker containers** (each its own IP) that run real tasks, driven through the whole scale curve | the "cloud": a container stands in for a provisioned instance | `RUN_CONTAINER_E2E=1` (+ Docker) |

The AWS control-plane tests ask *"does the manager drive AWS correctly?"*; the full e2e asks *"do real
workers provision, connect, scale, and return correct results?"* -- across container boundaries with
distinct IPs (the base for nested-client scenarios).

Every test is **opt-in** behind an environment gate and skips itself otherwise, so the default
`python -m unittest discover` build never pays their cost.

## AWS control plane

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
  interception bypasses. It does **not** boot instances or run tasks -- that is the full e2e's job.
  Community LocalStack does not implement ECS or Batch (Pro features), so those tests skip themselves
  against it; set a `LOCALSTACK_AUTH_TOKEN` to run them against Pro.

## Full e2e (Docker)

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
worker, and it is the base for richer e2e scenarios (slow/fast tasks, nested clients with distinct IPs,
custom tasks) that no in-process or cloud-mock test can reach. The provisioner is built for it: per-manager
`worker_manager_id` / container prefix, `workers_per_machine` / `max_machines`, and `boot_delay_seconds`
/ `shutdown_delay_seconds` knobs to simulate cloud latency.

**The self-contained worker image (no host-layout coupling).** Each machine runs a small image
(`worker.Dockerfile`, built by `_container_image.py`) that installs the host's freshly built wheel plus
the custom capnp/kj runtime libs, so a container is byte-identical to the scheduler it talks to -- the
wire protocol always matches. Only the repo is bind-mounted read-only, so a container worker can import
the test's task module (`tests.integration._tasks`, which the wheel does not ship). The container runtime
is abstracted (`_container_runtime.py`, Docker today, `SCALER_IT_CONTAINER_CLI`-swappable for podman), and
the workers reach the host scheduler + object storage over the docker-bridge gateway
(`SchedulerHarness(gateway=...)` binds `0.0.0.0` and advertises the gateway address). It needs a Docker
daemon, so it has its own gate (`RUN_CONTAINER_E2E=1`) and never runs in the standard CI lanes.

**Watching it run.** The harness starts the web GUI wired to the scheduler monitor and prints a
`web GUI: http://localhost:PORT` line; open it during a local run to watch the pool scale. (It is on in
CI too, just unwatched, to keep the setup identical.)

## Where is real task execution covered?

Neither moto nor community LocalStack boots the compute they "provision", so the AWS control-plane tests
cannot verify task execution. That is covered by the **full e2e** (`test_container_scaling_e2e.py`, in
containers) plus `tests/scheduler/test_scaling.py` in the **default suite** (a scheduler-from-zero +
native manager asserting dynamically provisioned worker *processes* return correct results, on every
push). A real *cloud* data plane -- provisioned instances that actually boot and connect back -- needs
LocalStack Pro (its ECS/EC2 Docker backend) or a comparable real backend, and is out of scope here.

## Running

```bash
# Install test dependencies (moto is in-process; the AWS control-plane tests need no Docker):
uv pip install -e '.[all]' --group dev

# AWS control plane (moto backend, the CI default):
RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v

# AWS control plane against a real LocalStack instead of moto (starts a container, runs, tears down):
./scripts/run_integration_localstack.sh          # DOCKER="sudo docker" ./... if the socket is root-only

# Full e2e (Docker): builds the worker image, brings up the stack + web GUI, runs the scaling scenarios:
./scripts/run_container_e2e.sh                    # DOCKER="sudo docker" ./... if the socket is root-only
```

**In CI:** the AWS control-plane tests run on the Linux lane with moto (see
`.github/actions/run-test/action.yml`). LocalStack runs on demand via the **`LocalStack AWS-Mock
Cross-Check`** workflow (`integration-localstack.yml`), and the full e2e runs on demand via the
**`Container Scaling E2E`** workflow (`container-e2e.yml`): trigger either from the Actions tab, or add
the **`localstack`** / **`container-e2e`** label to a PR.

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
