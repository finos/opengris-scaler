FROM mcr.microsoft.com/devcontainers/base:noble
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install cmake pkg-config gcc-14 g++-14 libgraphblas-dev python3-pip python3-venv pipx \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100 \
    && sudo -u vscode pipx install uv
COPY scripts/library_tool.sh /library_tool.sh
RUN /library_tool.sh capnp download \
    && /library_tool.sh capnp compile \
    && /library_tool.sh capnp install \
    && /library_tool.sh libuv download \
    && /library_tool.sh libuv compile \
    && /library_tool.sh libuv install \
    && /library_tool.sh openssl download \
    && /library_tool.sh openssl compile \
    && /library_tool.sh openssl install \
    && rm /library_tool.sh
