"""Task functions for the container e2e, kept in a light standalone module.

cloudpickle serializes a module-level function by qualified name, so the container workers deserialize
it by importing this module -- which must therefore stay free of the test module's unittest / container
-runtime imports. The container puts the repo root on PYTHONPATH so ``tests.integration._tasks`` resolves
(the editable install only exposes ``src/``); see ``_container_backend.ContainerWorkerProvisioner``.
"""

import time


def square(value: int) -> int:
    time.sleep(0.15)  # keep the burst non-instant so load stays sustained and scale-up is stable
    return value * value
