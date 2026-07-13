# End-to-end integration tests

Full-stack tests of OpenGRIS Scaler's scaling control loop. Every worker manager (ECS, ORB/EC2, Batch,
native, container, ...) is a `DeclarativeWorkerProvisioner` driven by the **same** scheduler command
(`setDesiredTaskConcurrency`) over the **same** wire protocol, so the suite splits by *how much is real*:

| Test | File(s) | What is real | What is mocked / simulated | Gate |
|------|---------|--------------|----------------------------|------|
| **AWS control plane** | `test_ecs_provisioning.py`, `test_ec2_orb_provisioning.py`, `test_batch_provisioning.py` | the real provisioner, the real boto3 request path, the real scheduler command object | AWS itself (moto, in-process). **Nothing boots and no task runs.** | `RUN_INTEGRATION_TESTS=1` |
| **Container-scaling e2e** | `e2e/` `Test_container`(`_waterfall`) | a real client, scheduler, and **real workers in real Docker containers** (each its own IP) that run real tasks | the "cloud": a container stands in for a provisioned instance (no AWS at all) | `RUN_CONTAINER_E2E=1` (+ Docker) |
| **ECS scaling e2e** | `e2e/` `Test_ecs` | the **shipped ECS worker manager** and its boto3 calls, **plus real workers in real ECS task containers** | AWS is a local [floci](https://github.com/floci-io/floci) emulator that actually launches `RunTask` containers | `RUN_FLOCI_E2E=1` (+ Docker) |
| **EC2 scaling e2e** | `e2e/` `Test_ec2` | the **shipped ORB/EC2 worker manager** and the ORB SDK, **plus real workers in real Amazon Linux 2023 instances** that install a current-source wheel and boot | AWS is the same floci emulator, launching `RunInstances` containers | `RUN_EC2_E2E=1` (+ Docker) |
| **Cross-backend waterfall e2e** | `e2e/` `Test_ecs_ec2` | **both** shipped managers (ECS + ORB/EC2) at different waterfall priorities on **one** scheduler; real ECS task containers and real EC2 instances run the work together | AWS is the same floci emulator, driving both `RunTask` and `RunInstances` | `RUN_CROSS_BACKEND_E2E=1` (+ Docker) |

The AWS control-plane tests ask *"does the manager drive AWS correctly?"*; the four Docker e2es ask *"do
real workers provision, connect, scale, and return correct results?"* -- the container e2e with no cloud at
all, the ECS and EC2 e2es through the *shipped* managers against an emulator that boots the compute for
real, and a cross-backend e2e that runs both cloud managers on one scheduler.

Every test is **opt-in** behind an environment gate and skips itself otherwise, so the default
`python -m unittest discover` build never pays their cost.

## AWS control plane (moto)

The provisioner's boto3 calls are asserted against **moto**, an in-process boto3 mock -- fast, free,
cross-platform, no Docker. It reimplements AWS below botocore and covers all three managers' control planes
(ECS, EC2, Batch + ECR). It does **not** boot the compute it "provisions", so these tests cover only the
control plane; the small gaps between moto and real AWS are centralised in `_aws_backend.py` (see notes
below) so the provisioner code runs unchanged.

## The Docker e2e framework: scenarios x backends

The Docker e2es are generated from a small framework (`tests/integration/e2e/`) that separates **what** is
tested from **which** worker manager runs it, so a scenario is written once and every backend reuses it:

* **Scenarios** (`e2e/scenarios.py`) -- backend-agnostic `(test, deployment)` functions asserting on the
  scaling curve: `burst_and_drain`, `work_spreads`, `rising_load`, `steady_load_stable`, and
  `waterfall_spills` (which needs >= 2 managers).
* **Backends** (`e2e/backends.py`) -- each supplies only the seams a scenario cannot know: a boot-latency
  matched `Profile`, how to build its task image, and how to provision one manager + observe its pool.
  `FlociEcsBackend`, `FlociEc2Backend`, `ContainerBackend`.
* **Topologies** (`e2e/matrix.py`) -- an ordered list of backends (1 = a single manager on the vanilla
  policy; N = a `waterfall_v1` deployment, priority = position, possibly cross-backend) plus the scenarios
  it runs. Each becomes one generated `unittest.TestCase` named for it -- `Test_ecs`, `Test_ec2`,
  `Test_container`, `Test_ecs_ec2`, `Test_container_waterfall` -- gated by its `RUN_*_E2E` flag, so each
  on-demand workflow (all running `tests.integration.e2e.test_scaling`) enables exactly its own rows.

Two unifications let one scenario drive them all: every backend runs its workers in host Docker containers,
so pool observation is a single prefix-scoped `docker ps` (`e2e/framework.py` `ContainerPool`); and tasks
are cloudpickled **by value** and tag their unit via `SCALER_IT_MACHINE_ID or hostname` (`e2e/tasks.py`),
covering the container backend's per-machine env id and the floci backends' hostname in one expression. The
`SchedulerHarness` health check (`assert_backend_processes_alive`) is deployment-level: if the scheduler or
a manager crashes -- or stays up but wedges the pool -- under churn, the test fails **naming the fault**
(from a tee'd scheduler log) instead of surfacing only a client `TimeoutError`.

## The backends

**Container (no cloud) -- `ContainerBackend`.** Launches container "machines", each a *fixed*
`baremetal_native` worker with its own IP (`_container_backend.py`), with no AWS at all -- the free, local
analog of a cloud manager booting an instance. It churns decisively under the vanilla policy (a test-double
provisioner), so `steady_load_stable` is a red tripwire on it. `Test_container_waterfall` runs two at
different priorities (distinct container prefixes) to exercise spill.

**ECS (floci) -- `FlociEcsBackend`.** Drives the shipped `ECSWorkerManager`
(`scaler.worker_manager_adapter.aws_raw.ecs`) unmodified against [floci](https://github.com/floci-io/floci),
a free local AWS emulator that -- unlike moto or community LocalStack -- actually launches each ECS
`RunTask` as a sibling Docker container (through the host docker socket) from a prebaked image. So the exact
production ECS path runs, boto3 merely pointed at floci via `AWS_ENDPOINT_URL`, and real workers connect
back and run work. Boots in seconds (`_floci.py`, `ecs.Dockerfile`).

**EC2 (floci) -- `FlociEc2Backend`.** Drives the shipped `ORBAWSEC2WorkerManager` and the real `orb-py` SDK
against the same floci, which launches each `RunInstances` as a real Amazon Linux 2023 container and runs
its UserData. The instance installs a **current-source** `manylinux` wheel (`scripts/build_cibuildwheel.sh`
builds it -- the plain `python -m build` wheel is `linux_x86_64` and will not run on AL2023's older glibc --
and the harness serves it over the docker-bridge gateway) and boots a worker. Minute-long boots, so the
profile raises the client/worker liveness timeouts to match. Two harness-side shims bridge floci vs a real
AMI/AWS with **no product change** (`_ec2_backend.py`): the launched image is augmented to the AL2023
baseline (floci's minimal image lacks `tar`, `ec2.Dockerfile`), and a botocore handler restores the
`InvalidLaunchTemplateName.NotFoundException` the ORB SDK expects.

**Cross-backend waterfall -- `Test_ecs_ec2`.** `waterfall_spills` on `[FlociEcsBackend` (priority 1,
capped)`, FlociEc2Backend` (priority 2)`]`: a sustained burst fills the fast ECS pool first and spills onto
real EC2 instances -- both cloud data planes running under one scheduler at once, attributed by container
prefix (`floci-ecs-*` vs `floci-ec2-*`).

**Watching a Docker e2e run.** The harness starts the web GUI wired to the scheduler monitor and prints a
`web GUI: http://localhost:PORT` line; open it during a local run to watch the pool scale. (It is on in CI
too, just unwatched, to keep the setup identical.)

## The self-contained worker image (no host-layout coupling)

The container and ECS backends run their workers from a small image (`worker.Dockerfile`, built by
`_container_image.py`) that installs the host's freshly built wheel plus the custom capnp/kj runtime libs,
so a container is byte-identical to the scheduler it talks to -- the wire protocol always matches. Tasks
travel **by value** for every backend (`e2e/tasks.py`), so no worker needs the repo: the ECS backend layers
only a thin `COMMAND`-exec entrypoint on top (`ecs.Dockerfile`), and the EC2 backend's instances just
install the current-source `manylinux` wheel over the gateway. The container runtime is abstracted
(`_container_runtime.py`, Docker today, `SCALER_IT_CONTAINER_CLI`-swappable for podman), and the workers
reach the host scheduler + object storage over the docker-bridge gateway (`SchedulerHarness(gateway=...)`
binds `0.0.0.0` and advertises the gateway address). All four topologies need a Docker daemon, so each has
its own gate and none run in the standard CI lanes.

## Where is real task execution covered?

moto does not boot the compute it "provisions", so the AWS control-plane tests cannot verify task
execution. That is covered by the four **Docker e2es** plus `tests/scheduler/test_scaling.py` in the
**default suite** (a scheduler-from-zero + native manager asserting dynamically provisioned worker
*processes* return correct results, on every push). The floci ECS and EC2 e2es are the closest to a real
cloud data plane -- the shipped managers launching real task containers / instances -- without a paid tier
or a real AWS account.

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

# EC2 scaling e2e (floci + Docker): builds a current-source manylinux wheel (cibuildwheel, several minutes)
# if absent, then drives the shipped ORB/EC2 manager on real AL2023 instances:
./scripts/run_ec2_e2e.sh                         # DOCKER="sudo docker" ./... if the socket is root-only

# Cross-backend waterfall e2e (floci + Docker): builds the ECS image + a current-source manylinux wheel,
# then drives the ECS (priority 1) and ORB/EC2 (priority 2) managers on one scheduler and watches the spill:
./scripts/run_cross_backend_e2e.sh               # DOCKER="sudo docker" ./... if the socket is root-only
```

Each runner just builds its image(s), sets its `RUN_*_E2E` gate, and runs the framework module, so you can
drive it directly -- a whole topology, or one scenario -- once the images/wheel exist:

```bash
# one topology (RUN_*_E2E selects which class is enabled; others skip):
RUN_FLOCI_E2E=1 SCALER_IT_CONTAINER_CLI="sudo docker" \
  python -m unittest tests.integration.e2e.test_scaling.Test_ecs -v
# one scenario:
RUN_CONTAINER_E2E=1 SCALER_IT_CONTAINER_CLI="sudo docker" \
  python -m unittest tests.integration.e2e.test_scaling.Test_container.test_steady_load_stable -v
```

**In CI:** the AWS control-plane tests run with moto on the Linux lane (`.github/actions/run-test/action.yml`,
Python 3.10) and again on a dedicated **Python 3.11** job in `build-and-test.yml` (so the ORB/EC2 test,
which needs 3.11+, actually executes). The Docker e2es run on demand -- the **`Container Scaling E2E`**
(`container-e2e.yml`), **`Floci ECS Scaling E2E`** (`floci-e2e.yml`), **`Floci EC2 Scaling E2E`**
(`ec2-e2e.yml`), and **`Floci Cross-Backend Waterfall E2E`** (`cross-backend-e2e.yml`) workflows --
triggered from the Actions tab or by adding the **`container-e2e`** / **`floci-e2e`** / **`ec2-e2e`** /
**`cross-backend-e2e`** label to a PR.

### Parameterizing an on-demand run

Each on-demand workflow exposes `workflow_dispatch` inputs (and the same knobs work locally as env vars, since
the run scripts inherit the environment). Blank/unset always falls back to the backend's tuned default:

| Input / env var | What it sets |
|-----------------|--------------|
| `num_tasks` / `SCALER_IT_NUM_TASKS` | tasks per burst wave (and per held-load burst) |
| `task_seconds` / `SCALER_IT_TASK_SECONDS` | the `time.sleep` in each task |
| `policy_content` / `SCALER_IT_WATERFALL_POLICY` | the **raw** `waterfall_v1` policy (waterfall topologies only) |

`SCALER_IT_WATERFALL_POLICY` is passed to the scheduler verbatim -- one rule per line,
`priority,worker_manager_id[,max_task_concurrency]` (`#` comments allowed), the same format `deploy()` generates.
It lets you set which managers run, their priorities, and their caps without touching code; a cap in the policy
also resizes that manager's own pool, so the scheduler's spill threshold and the provisioned pool stay in step.
The manager ids are `wm-<name>-p<position>` (position = priority, 1 = highest): `wm-ecs-p1` + `wm-ec2-p2` for the
cross-backend topology, `wm-container-p1` + `wm-container-p2` for `Test_container_waterfall`. `deploy()` logs the
ids and the effective policy at start-up (`[deploy] waterfall_v1 policy for managers [...]`), so a run tells you
exactly what to reference; a policy that names an id no manager registers under fails fast. The override is
ignored (with a logged note) on the single-manager topologies, which run the vanilla policy. Raising
`task_seconds` well past the default keeps the timeouts fixed, so it is meant mainly for the waterfall runs,
where longer tasks make the top pool saturate and spill deterministically.

```bash
# a bigger, slower cross-backend waterfall with ecs (priority 1, cap 4) spilling to ec2 (priority 2):
RUN_CROSS_BACKEND_E2E=1 SCALER_IT_CONTAINER_CLI="sudo docker" \
  SCALER_IT_NUM_TASKS=60 SCALER_IT_TASK_SECONDS=0.3 \
  SCALER_IT_WATERFALL_POLICY=$'1,wm-ecs-p1,4\n2,wm-ec2-p2' \
  python -m unittest tests.integration.e2e.test_scaling.Test_ecs_ec2 -v
```

## Adding a scenario, backend, or topology

The three axes are independent -- extend one without touching the others:

* **A scenario** -- add a `(test_case, deployment)` function to `e2e/scenarios.py`: read timing off
  `deployment.profile`, submit via `deployment.tasks`, and observe with `deployment.running()` /
  `running_names()` or per-manager `deployment.pools`. Set its `min_managers`, then list it on the
  topologies that should run it in `e2e/matrix.py`. It now runs on every listed backend.
* **A backend** -- add a class to `e2e/backends.py` satisfying `WorkerManagerBackend`: a `Profile` matched to
  its boot latency, `ensure_image()`, and `provision()` returning a `ManagerHandle` with a prefix-scoped
  `ContainerPool`. Add a `Topology` for it in `e2e/matrix.py` with a gate, and every existing scenario runs
  on it for free. If it needs new shared infra (an emulator, a wheel server), extend `deploy()` behind a
  `needs_*` flag.
* **A topology** -- add a `Topology` to `TOPOLOGIES` in `e2e/matrix.py`: an ordered list of `ManagerSpec`s
  (priority = position; cap the higher tiers to force spill), the scenarios it runs, and its gate. A
  cross-backend waterfall is just `[ManagerSpec(BackendA(), cap=N), ManagerSpec(BackendB())]`; the generated
  class is `Test_<name>`.

Everything else -- pool observation, by-value task tagging, the scheduler harness, the crash/wedge
diagnostic, and the workflows (all running the gated `tests.integration.e2e.test_scaling`) -- is shared, so
a new axis needs no wiring beyond the above.

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
