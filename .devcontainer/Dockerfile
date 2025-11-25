FROM mcr.microsoft.com/devcontainers/base:noble
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install cmake pkg-config gcc-14 g++-14 python3-pip python3-venv pipx openssh-server sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100 \
    && sudo -u vscode pipx install uv \
    && mkdir -p /run/sshd \
    && echo 'vscode:vscode' | chpasswd \
    && echo 'vscode ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
COPY scripts/library_tool.sh /library_tool.sh
RUN /library_tool.sh boost download \
    && /library_tool.sh boost compile \
    && /library_tool.sh boost install \
    && /library_tool.sh capnp download \
    && /library_tool.sh capnp compile \
    && /library_tool.sh capnp install \
    && rm /library_tool.sh

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
