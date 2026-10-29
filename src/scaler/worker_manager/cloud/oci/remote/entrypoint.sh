#!/bin/bash
set -e

uv python install "${PYTHON_VERSION}"
uv venv --python "${PYTHON_VERSION}" /opt/opengris-scaler

printf '%s\n' "${PYTHON_REQUIREMENTS}" > /tmp/requirements.txt

# If any requirement is a VCS source URL, install C++ build dependencies first.
# This mirrors what the EC2 user data script does for source builds of scaler.
if grep -qE 'git\+|@ git\+' /tmp/requirements.txt; then
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl git cmake make perl gcc-14 g++-14 pkg-config \
        libcapnp-dev capnproto \
        libuv1-dev
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100
    rm -rf /var/lib/apt/lists/*

    # Ubuntu's libssl-dev lacks the OpenSSL CMake package scaler needs, so build OpenSSL with the repo's library_tool.sh
    read -r clone_url clone_branch < <(/opt/opengris-scaler/bin/python - <<'EOF'
import re, urllib.parse
url = urllib.parse.urlsplit(re.search(r"git\+([^\s;]+)", open("/tmp/requirements.txt").read()).group(1))
path, _, branch = url.path.partition("@")
print(urllib.parse.urlunsplit(url._replace(path=path, fragment="")), branch)
EOF
)
    git clone --depth 1 ${clone_branch:+--branch "${clone_branch}"} "${clone_url}" /tmp/scaler-src
    for step in download compile install; do /tmp/scaler-src/scripts/library_tool.sh openssl "${step}"; done
fi

uv pip install --no-cache -q --python /opt/opengris-scaler -r /tmp/requirements.txt

ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

echo "Executing: ${COMMAND}"
exec bash -c "${COMMAND}"
