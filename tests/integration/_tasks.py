"""Task functions for the container e2e, kept in a light standalone module.

cloudpickle serializes a module-level function by qualified name, so the container workers deserialize
it by importing this module -- which must therefore stay free of the test module's unittest / container
-runtime imports. The container puts the repo root on PYTHONPATH so ``tests.integration._tasks`` resolves
(the wheel only ships ``src/``); see ``_container_backend.ContainerWorkerProvisioner``.
"""

import os
import time

# Keep tasks non-instant so load stays sustained and scale-up is stable, but light enough that many can
# oversubscribe a few cores.
_TASK_SECONDS = 0.15


def square(value: int) -> int:
    time.sleep(_TASK_SECONDS)
    return value * value


def square_on_machine(value: int):
    """Square, and also report which container 'machine' ran it via SCALER_IT_MACHINE_ID (set per machine
    by the provisioner). Lets a test see work spread across machines / managers."""
    time.sleep(_TASK_SECONDS)
    return value * value, os.environ.get("SCALER_IT_MACHINE_ID", "?")
