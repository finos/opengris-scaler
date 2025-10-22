FROM ubuntu:rolling

RUN apt-get update && apt-get install -y \
    bash curl build-essential pkg-config git ca-certificates

RUN set -eux; \
    curl -O https://capnproto.org/capnproto-c++-1.1.0.tar.gz; \
    tar zxf capnproto-c++-1.1.0.tar.gz; \
    cd capnproto-c++-1.1.0; \
    ./configure; \
    make -j$(nproc); \
    make install; \
    cd ..;

# Create non-root user matching VS Code devcontainers defaults
RUN useradd -m -s /bin/bash vscode \
    && mkdir -p /home/vscode/.cache/ccache \
    && chown -R vscode:vscode /home/vscode

USER vscode

# ccache convenience
RUN ccache -o max_size=${CCACHE_MAXSIZE} || true

ENV PATH="/usr/lib/ccache:${PATH}"

WORKDIR /workspaces
