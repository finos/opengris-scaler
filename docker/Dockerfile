ARG BASE_IMAGE=alpine:3.23
FROM ghcr.io/astral-sh/uv:latest AS uv
FROM ${BASE_IMAGE}

RUN apk add --no-cache \
    ca-certificates \
    capnproto \
    libuv \
    libstdc++ \
    libgcc \
    openssl

COPY --from=uv /uv /usr/local/bin/uv

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

# entrypoint reads PYTHON_VERSION, PYTHON_REQUIREMENTS, and COMMAND from env vars
ENTRYPOINT ["/app/entrypoint.sh"]
