# AWS end-to-end integration tests

These tests exercise the real worker-manager provisioning paths against a mocked AWS
control plane. They are opt-in so the default local `python -m unittest discover`
run stays fast:

```bash
RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v
```

CI runs this suite on the Linux lane with moto. LocalStack is available through the
helper script:

```bash
./scripts/run_integration_localstack.sh
```

If your user cannot reach the Docker socket:

```bash
DOCKER="sudo docker" ./scripts/run_integration_localstack.sh
```

## What Is Covered

| File | Worker manager | AWS service | Backend support |
|------|----------------|-------------|-----------------|
| `test_ecs_provisioning_moto.py` | ECS (`aws_raw`) | ECS | moto, LocalStack Pro |
| `test_ec2_orb_provisioning.py` | ORB/EC2 (`orb_aws_ec2`) | EC2 | moto, free LocalStack, LocalStack Pro |
| `test_batch_provisioning_moto.py` | AWS Batch (`aws_hpc`) | Batch + S3 + IAM | moto, LocalStack Pro |

The tests use the same scheduler command object that production scaling emits
(`setDesiredTaskConcurrency`) and then assert against the mocked AWS backend's
real state. They cover scale-up, scale-down, termination, caps, resource
discovery, and idempotent infrastructure provisioning.

Neither moto nor community LocalStack boots the compute they create. These tests
therefore verify the AWS control plane: boto3 parameters, resource discovery,
lifecycle tracking, and cleanup. Real task execution still needs a local
process-based test, LocalStack Pro with a Docker-backed service, moto's Batch
Docker backend, or a real AWS account.

## Backends

`SCALER_E2E_AWS_BACKEND` selects the backend:

```bash
# Default: in-process moto backend.
RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v

# LocalStack backend, managed by the helper script.
./scripts/run_integration_localstack.sh
```

Backend notes:

- moto is in-process, free, and does not require Docker. It supports the ECS and
  Batch control-plane coverage used by CI.
- free community LocalStack supports EC2, so the ORB/EC2 test can run there.
- ECS and Batch are LocalStack Pro features. On community LocalStack, those
  tests skip themselves with a clear message.

## Dependencies

Install the package with the integration extra when running this suite locally:

```bash
uv pip install -e '.[all,integration]'
```

`tests/integration/_aws_backend.py` centralizes compatibility shims needed to run
the unchanged provisioner code against moto and LocalStack.
