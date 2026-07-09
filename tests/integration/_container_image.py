"""Build the self-contained worker image the container-scaling e2e runs its "machines" from.

The image bakes in the host's freshly built wheel plus the custom capnp/kj runtime libs (see
``worker.Dockerfile``), so a container is byte-identical to the scheduler it talks to without any
host-layout bind-mounts. The only thing still mounted at run time is the repo, so a container worker
can import the test's task module (``tests.integration._tasks``), which is not part of the wheel.

Portable across the dev host (libs in ``/usr/local/lib``) and CI (libs under
``$SCALER_THIRDPARTY_PREFIX/lib``); ``PYTHON_VERSION`` / the wheel are taken from the current
interpreter, so the same environment that built the wheel builds a matching image.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
import tempfile
from typing import List

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_DOCKERFILE = os.path.join(_HERE, "worker.Dockerfile")
_ECS_DOCKERFILE = os.path.join(_HERE, "ecs.Dockerfile")
_ECS_ENTRYPOINT = os.path.join(_HERE, "ecs_entrypoint.sh")

DEFAULT_IMAGE_TAG = os.environ.get("SCALER_IT_WORKER_IMAGE", "scaler-it-worker:local")
# The ECS e2e task image: the worker image plus an entrypoint that execs the provisioner's COMMAND env var.
DEFAULT_ECS_IMAGE_TAG = os.environ.get("SCALER_IT_ECS_IMAGE", "scaler-it-ecs-worker:local")
_DEFAULT_BASE_IMAGE = os.environ.get("SCALER_IT_BASE_IMAGE", "ubuntu:26.04")

# The scaler extensions dynamically link the custom capnp/kj libs; ymq statically links libuv, so the
# capnp/kj family is all the worker needs. openssl/libuv globs are copied when present but not required.
_REQUIRED_LIB_GLOBS = ("libcapnp*.so*", "libkj*.so*")
_OPTIONAL_LIB_GLOBS = ("libuv*.so*", "libssl*.so*", "libcrypto*.so*")


def _cli() -> List[str]:
    return os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()


def _libs_dir() -> str:
    """Where the custom capnp/kj shared libs live: the CI third-party prefix if set, else /usr/local/lib."""
    prefix = os.environ.get("SCALER_THIRDPARTY_PREFIX")
    return os.path.join(prefix, "lib") if prefix else "/usr/local/lib"


def _wheel_path() -> str:
    wheels = sorted(glob.glob(os.path.join(_REPO_ROOT, "dist", "*.whl")))
    if not wheels:
        raise RuntimeError(
            "no wheel found under dist/; build one first with `python -m build --wheel` "
            "(CI's build-and-install-wheel step already does this)"
        )
    return wheels[-1]


def _python_version() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def image_exists(tag: str = DEFAULT_IMAGE_TAG) -> bool:
    return subprocess.run([*_cli(), "image", "inspect", tag], capture_output=True).returncode == 0


def ensure_worker_image(tag: str = DEFAULT_IMAGE_TAG, rebuild: bool = False) -> str:
    """Return an image tag that runs the current scaler build, building it only if missing (or forced).

    Reuse makes a bare ``python -m unittest`` fast; pass ``rebuild=True`` (the local run script does) after
    changing product code so a stale image does not silently run the old build.
    """
    if rebuild or not image_exists(tag):
        build_worker_image(tag)
    return tag


def build_worker_image(tag: str = DEFAULT_IMAGE_TAG) -> str:
    libs_dir = _libs_dir()
    wheel = _wheel_path()
    with tempfile.TemporaryDirectory(prefix="scaler-it-image-") as context:
        wheel_dir = os.path.join(context, "wheel")
        libs_stage = os.path.join(context, "libs")
        os.mkdir(wheel_dir)
        os.mkdir(libs_stage)
        shutil.copy(wheel, wheel_dir)

        required = _stage_libs(libs_dir, _REQUIRED_LIB_GLOBS, libs_stage)
        if not required:
            raise RuntimeError(f"no capnp/kj runtime libs (e.g. libcapnp*.so) found under {libs_dir}")
        _stage_libs(libs_dir, _OPTIONAL_LIB_GLOBS, libs_stage)

        subprocess.run(
            [
                *_cli(),
                "build",
                "-f",
                _DOCKERFILE,
                "--build-arg",
                f"BASE_IMAGE={_DEFAULT_BASE_IMAGE}",
                "--build-arg",
                f"PYTHON_VERSION={_python_version()}",
                "-t",
                tag,
                context,
            ],
            check=True,
        )
    return tag


def ensure_ecs_worker_image(
    tag: str = DEFAULT_ECS_IMAGE_TAG, base: str = DEFAULT_IMAGE_TAG, rebuild: bool = False
) -> str:
    """Return an ECS task image (the worker image plus the COMMAND-exec entrypoint), building it and its
    base only if missing (or forced). ``rebuild`` rebuilds both so a wheel change is never run stale."""
    ensure_worker_image(base, rebuild=rebuild)
    if rebuild or not image_exists(tag):
        build_ecs_worker_image(tag, base)
    return tag


def build_ecs_worker_image(tag: str = DEFAULT_ECS_IMAGE_TAG, base: str = DEFAULT_IMAGE_TAG) -> str:
    # Stage just the entrypoint so the build context stays tiny (not the whole tests/integration dir).
    with tempfile.TemporaryDirectory(prefix="scaler-it-ecs-image-") as context:
        shutil.copy(_ECS_ENTRYPOINT, context)
        subprocess.run(
            [*_cli(), "build", "-f", _ECS_DOCKERFILE, "--build-arg", f"BASE_IMAGE={base}", "-t", tag, context],
            check=True,
        )
    return tag


def _stage_libs(libs_dir: str, patterns: tuple, destination: str) -> int:
    """Copy matching libs (preserving the ``libX.so -> libX-1.1.0.so`` symlinks) into the build context."""
    copied = 0
    for pattern in patterns:
        for lib in glob.glob(os.path.join(libs_dir, pattern)):
            shutil.copy2(lib, os.path.join(destination, os.path.basename(lib)), follow_symlinks=False)
            copied += 1
    return copied


if __name__ == "__main__":
    print(build_worker_image())
