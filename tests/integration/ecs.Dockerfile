# Thin ECS task image for the floci-backed ECS e2e (see tests/integration/README.md).
#
# The shipped ECSWorkerProvisioner delivers the worker command through the COMMAND env var rather than the
# container command, so this wraps the self-contained worker image (worker.Dockerfile) with an entrypoint
# that execs $COMMAND. The scaler wheel is already baked into the base, so no install happens at task start
# (the test leaves ecs_python_requirements empty). Built by _container_image.ensure_ecs_worker_image.
ARG BASE_IMAGE=scaler-it-worker:local
FROM ${BASE_IMAGE}

COPY ecs_entrypoint.sh /usr/local/bin/ecs_entrypoint.sh
RUN chmod +x /usr/local/bin/ecs_entrypoint.sh

ENTRYPOINT ["/usr/local/bin/ecs_entrypoint.sh"]
