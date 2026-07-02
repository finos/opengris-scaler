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

# Run the whole skeleton:
python -m unittest discover -s tests/integration -t . -v

# Or a single layer:
python -m unittest tests.integration.test_ecs_provisioning_moto -v
python -m unittest tests.integration.test_dynamic_local_e2e -v
```

## Choosing the mocked-AWS backend

The AWS backend is selected by `SCALER_E2E_AWS_BACKEND` (default `moto`) and is the only thing
that changes between backends — the test body is identical:

* **`moto`** (default): in-process boto3 mock. No Docker, no network, runs on Linux/macOS/Windows.
  Best for CI. Mocks the ECS control plane only.
* **`localstack`**: point `AWS_ENDPOINT_URL` at a running LocalStack (default
  `http://localhost:4566`) and run:
  ```bash
  docker run --rm -d -p 4566:4566 localstack/localstack
  SCALER_E2E_AWS_BACKEND=localstack python -m unittest tests.integration.test_ecs_provisioning_moto -v
  ```

### Is there a better tool than LocalStack?

For CI, **moto is the better default**: it is a pip dependency (no Docker daemon), runs
in-process, is fast, and is cross-platform. Community LocalStack needs Docker and — like moto —
still does not boot the provisioned containers, so it buys little over moto for these tests.

The only way to get a **true cloud data-plane** (provisioned "instances" that boot and connect
back as real workers) with a mock is:

* **LocalStack Pro** — its ECS/EC2 Docker backend actually launches containers. Point the ECS
  task image at a real `opengris-scaler` image and the provisioned tasks will connect back to
  the scheduler. This is the highest-fidelity option but requires a licence + Docker.
* **moto standalone server + AWS Batch** — moto's Batch backend executes jobs in local Docker
  containers; the `aws_hpc` (Batch) worker manager could be pointed at it for a real boot.

Both are heavier and Docker-bound, so they are documented here as opt-in extensions rather than
wired into the default skeleton.

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
