# End-to-end integration tests

A skeleton for full-stack, end-to-end tests of OpenGRIS Scaler with a worker manager
that provisions "cloud instances", using a mocked AWS control plane.

These tests are **not** run by the default `python -m unittest discover` CI job unless the
extra dependencies are installed; each test `skipUnless(...)` its requirements, so they are
safe to keep in-tree without slowing the standard build.

## What is covered

The worker managers (ECS, ORB/EC2, Batch, native, ...) are all
`DeclarativeWorkerProvisioner`s driven by the **same** scheduler command
(`setDesiredTaskConcurrency`) over the **same** wire protocol. The skeleton splits the loop
into two complementary layers so each can be tested deterministically:

| File | Layer | What is real | What is mocked |
|------|-------|--------------|----------------|
| `test_ecs_provisioning_moto.py` | **control plane** | the real `ECSWorkerProvisioner`, the real boto3 request path, the real scheduler command object | AWS (moto/LocalStack); provisioned containers do **not** boot |
| `test_dynamic_local_e2e.py` | **data plane** | real scheduler + object storage + `WorkerManagerRunner` (over the wire) + provisioned worker processes + task execution | nothing — provisioning is local processes instead of cloud instances |

Together they exercise the entire path: client submits work -> scheduler scaling policy emits
`setDesiredTaskConcurrency` -> worker manager -> provisioner -> AWS API / real workers.

### Why a "local process" data-plane layer?

Neither **moto** nor **community LocalStack** actually boots the containers/instances they
"provision" (they mock the control-plane API only), so a cloud-mocked test cannot verify real
task execution. `test_dynamic_local_e2e.py` closes that gap with the `NativeWorkerProvisioner`,
which provisions real worker *processes* through the identical scheduler->manager->provisioner
control loop — so "did provisioning actually run work?" is answered with real components.

## Running

```bash
# Install the test dependencies (moto is in-process; no Docker required):
uv pip install -e '.[all,integration]'

# Run the whole skeleton (moto backend, the default):
RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v

# Or a single layer:
RUN_INTEGRATION_TESTS=1 python -m unittest tests.integration.test_ecs_provisioning_moto -v
RUN_INTEGRATION_TESTS=1 python -m unittest tests.integration.test_dynamic_local_e2e -v
```

The suite is opt-in (`RUN_INTEGRATION_TESTS=1`), so the default `unittest discover` skips it. CI runs
it on the **Linux** lane only, with the moto backend (see `.github/actions/run-test/action.yml`).

To run against a real LocalStack instead, use the helper (starts a container, runs, tears down):

```bash
./scripts/run_integration_localstack.sh
# if your user can't reach the docker socket:
DOCKER="sudo docker" ./scripts/run_integration_localstack.sh
```

## Choosing the mocked-AWS backend

The AWS backend is selected by `SCALER_E2E_AWS_BACKEND` (default `moto`) and is the only thing
that changes between backends — the test body is identical:

* **`moto`** (default): in-process boto3 mock. No Docker, no network, runs on Linux/macOS/Windows.
  Best for CI. Implements the ECS control plane in-process.
* **`localstack`**: point `AWS_ENDPOINT_URL` at a running LocalStack (default
  `http://localhost:4566`). Use `scripts/run_integration_localstack.sh` (above), which manages the
  container for you.

### Do moto and LocalStack test different things? Which should I use?

For **this** skeleton the control-plane assertions are the same on both backends, but they exercise
different layers:

* **moto** reimplements AWS *in-process* and intercepts boto3 below botocore — fast, free,
  cross-platform, no Docker. It **implements ECS**, so it runs the whole ECS control-plane test.
* **LocalStack** is a *separate service* boto3 talks to over real HTTP, so it also exercises the
  serialization/endpoint/region path that moto's in-process interception bypasses.

The important caveat, confirmed by running it: **community LocalStack does NOT implement ECS — ECS is
a LocalStack Pro feature.** So on the free community image the ECS control-plane test **skips itself**
(the harness detects the "not implemented / pro feature" error; the EC2 seeding still runs). That
makes **moto the only *free* backend that fully runs the ECS skeleton**, which is why CI uses it.

The value of the LocalStack seam is therefore:

* a higher-fidelity cross-check of the real HTTP path (for services community LocalStack *does*
  support, e.g. EC2), and
* the on-ramp to a **true cloud data-plane**. The only way to get provisioned "instances" that
  actually boot and connect back as real workers via a mock is:
  * **LocalStack Pro** — its ECS/EC2 Docker backend launches real containers. Export
    `LOCALSTACK_AUTH_TOKEN` and `scripts/run_integration_localstack.sh` uses the Pro image so the
    ECS tests run (point the ECS task image at a real `opengris-scaler` image and the tasks connect
    back to the scheduler).
  * **moto standalone server + AWS Batch** — moto's Batch backend executes jobs in local Docker
    containers; the `aws_hpc` (Batch) worker manager could be pointed at it for a real boot.

Both real-boot paths are heavier and Docker-bound, so they are opt-in extensions, not the default.

## moto compatibility notes

`tests/integration/_aws_backend.py` centralises the small gaps between moto and real AWS so the
provisioner code is exercised **unchanged**:

1. `MOTO_IAM_LOAD_MANAGED_POLICIES=true` — so the provisioner can attach the AWS-managed
   `AmazonECSTaskExecutionRolePolicy`.
2. The ECS environment (VPC with DNS hostnames, subnet, security group, cluster, and a task
   definition with container-level memory) is pre-seeded, exactly as a real AWS account would
   already have it. This also lets the provisioner *discover* the task definition instead of
   registering one (which trips a moto Fargate memory-summing quirk).
3. A botocore `before-parameter-build.ecs.RunTask` handler injects a default `securityGroups`,
   which moto requires but real AWS defaults.

## Extending the skeleton

* **ORB / EC2** — the ORB manager provisions via the `orb-py` SDK (not raw boto3), so mock at
  the SDK level, or use moto EC2 if ORB is pointed at raw `RunInstances`.
* **AWS Batch (`aws_hpc`)** — mock with `moto[batch]`; its Docker backend can boot real jobs.
* **Full scheduler-driven cloud loop** — run a real scheduler and a `WorkerManagerRunner`
  wrapping a cloud provisioner in-process (so moto intercepts its boto3 calls), submit work, and
  assert on the mocked AWS backend's task state. The two current files already cover the two
  halves of this loop deterministically.
