#!/bin/bash
#
# Cross-check the AWS worker-manager control plane against a real LocalStack container -- the real
# HTTP/endpoint path the default in-process moto backend bypasses. LocalStack is purely an AWS API mock:
# nothing boots and no task runs (that is the container-scaling e2e's job). Same AWS control-plane tests,
# just pointed at LocalStack instead of moto.
#
# Usage:
#       ./scripts/run_integration_localstack.sh
#
# Notes:
#   * ECS is a LocalStack *Pro* feature. With the free community image the ECS control-plane test
#     skips itself (the EC2 seam is still exercised). Export LOCALSTACK_AUTH_TOKEN to use the Pro
#     image so the ECS tests actually run.
#   * If your user cannot reach the Docker socket, run with `DOCKER="sudo docker" ./scripts/...`.
#   * Requires the package installed with the dev group: uv pip install -e '.[all]' --group dev
#
set -euo pipefail

DOCKER="${DOCKER:-docker}"
CONTAINER_NAME="${LOCALSTACK_CONTAINER_NAME:-scaler-localstack-it}"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
PORT="${LOCALSTACK_PORT:-4566}"
PYTHON="${PYTHON:-python}"

if [[ -n "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
    IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack-pro:latest}"
    AUTH_ENV=(-e "LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN}")
    echo "Using LocalStack Pro image ${IMAGE} (ECS tests will run)."
else
    # Pinned community version: ':latest' community images have started requiring a license token.
    IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack:3.8}"
    AUTH_ENV=()
    echo "Using free community image ${IMAGE}. ECS is Pro-only, so ECS tests will SKIP."
    echo "Set LOCALSTACK_AUTH_TOKEN to run them against LocalStack Pro."
fi

cleanup() {
    echo "Stopping LocalStack container ${CONTAINER_NAME}..."
    ${DOCKER} rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

${DOCKER} rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
echo "Starting LocalStack (${IMAGE})..."
${DOCKER} run -d --name "${CONTAINER_NAME}" -p "${PORT}:4566" "${AUTH_ENV[@]}" "${IMAGE}" >/dev/null

echo "Waiting for LocalStack to become ready..."
LOCALSTACK_READY=0
for _ in $(seq 1 60); do
    if curl -sf "${ENDPOINT}/_localstack/health" >/dev/null 2>&1; then
        echo "LocalStack is ready."
        LOCALSTACK_READY=1
        break
    fi
    sleep 2
done

if [[ "${LOCALSTACK_READY}" != "1" ]]; then
    echo "LocalStack did not become ready at ${ENDPOINT} within 120 seconds." >&2
    ${DOCKER} logs "${CONTAINER_NAME}" >&2 || true
    exit 1
fi

export SCALER_E2E_AWS_BACKEND=localstack
export RUN_INTEGRATION_TESTS=1
export AWS_ENDPOINT_URL="${ENDPOINT}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

echo "Running integration tests against LocalStack..."
"${PYTHON}" -m unittest discover -s tests/integration -t . -v
