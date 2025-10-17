# syntax=docker/dockerfile:1.7
FROM ubuntu:rolling

# Preinstall multiple Python versions; override at build time
ARG PYTHON_VERSIONS="3.8.20 \
                     3.10.18 \
                     3.11.13 \
                     3.12.11 \
                     3.13.8 \
                     3.14.0"
ARG SCALER_REPO="https://github.com/1597463007/scaler.git"
ARG SCALER_REF="ec2_dev"

# ---- System deps for CPython on musl + small QoL tools ----
RUN apt-get update && apt-get install -y \
    bash curl build-essential pkg-config git ca-certificates

# ---- capnproto (build dependency of pycapnp) ----
RUN set -eux; \
    curl -O https://capnproto.org/capnproto-c++-1.1.0.tar.gz; \
    tar zxf capnproto-c++-1.1.0.tar.gz; \
    cd capnproto-c++-1.1.0; \
    ./configure; \
    make -j$(nproc); \
    make install; \
    cd ..;

# ---- uv (fast installer/resolver) ----
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

RUN set -eux; \
    uv python install ${PYTHON_VERSIONS}; \
    git clone --branch "${SCALER_REF}" --depth 1 "${SCALER_REPO}" scaler; \
    cd scaler; \
    for v in ${PYTHON_VERSIONS}; do \
        uv venv /opt/venvs/$v --python $v; \
        . /opt/venvs/$v/bin/activate; \
        uv pip install --upgrade pip setuptools wheel; \
        uv pip install -e .; \
        deactivate; \
    done;

# ---- Runtime env knobs ----
# e.g., "3.12.5"; must be among PYTHON_VERSIONS
ENV PYTHON_VERSION=""
ENV PYTHON_REQUIREMENTS=""
ENV COMMAND=""
ENV SSL_CERT_DIR=/etc/ssl/certs SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# ---- Entrypoint ----
COPY <<'ENTRYPOINT_SH' /entrypoint.sh
#!/usr/bin/env bash
set -euo pipefail

main() {
  echo "Environment Variables:"
  export

  if [[ ! -d "/opt/venvs/${PYTHON_VERSION}" ]]; then
      echo "[error] Virtualenv `/opt/venvs/${PYTHON_VERSION}` not found." >&2
      echo "[info] Available venvs:" >&2
      ls -1 /opt/venvs || true
      exit 1
    fi

  # If requirements provided, create venv & install using uv
  if [[ -n "${PYTHON_REQUIREMENTS:-}" ]]; then
    tmpreq="$(mktemp)"
    printf "%s\n" "${PYTHON_REQUIREMENTS}" | tr ' ,;' '\n' | sed -E '/^\s*$/d' > "${tmpreq}"
    . /opt/venvs/${PYTHON_VERSION}/bin/activate;
    uv pip install -v -r "${tmpreq}"
    deactivate;
    rm -f "${tmpreq}"
  fi

  if [[ -z "${COMMAND:-}" ]]; then
    echo "[info] No COMMAND provided; opening shell."
    python -V || true
    which python || true
    exec bash
  else
    echo "[info] Executing COMMAND: ${COMMAND}"
    exec bash -lc ". /opt/venvs/${PYTHON_VERSION}/bin/activate; ${COMMAND}"
  fi
}

main "$@"
ENTRYPOINT_SH

RUN chmod +x /entrypoint.sh
SHELL ["/bin/bash", "-lc"]
ENTRYPOINT ["/entrypoint.sh"]
