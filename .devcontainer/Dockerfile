FROM mcr.microsoft.com/devcontainers/base:noble
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install cmake pkg-config gcc-14 g++-14 python3-pip python3-venv pipx openssh-server sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100 \
    && sudo -u vscode pipx install uv \
    && sudo -u vscode pipx install awscli \
    && mkdir -p /run/sshd /home/vscode/.ssh \
    && chown vscode:vscode /home/vscode/.ssh \
    && chmod 700 /home/vscode/.ssh \
    && echo 'vscode ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config \
    && sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config \
    && echo 'PubkeyAuthentication yes' >> /etc/ssh/sshd_config
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
