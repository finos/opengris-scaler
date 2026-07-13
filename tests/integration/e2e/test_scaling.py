"""Generated backend x scenario scaling e2es. The cases (``Test_ecs``, ``Test_ec2``, ``Test_container``,
``Test_ecs_ec2``, ``Test_container_waterfall``) are materialized from the matrix in e2e/matrix.py; each is
gated by a ``RUN_*_E2E`` flag, so an on-demand workflow enables only the topologies behind its gate
(``RUN_CONTAINER_E2E`` covers both the container and container-waterfall cases) and skips the rest.
"""

from __future__ import annotations

import unittest

from tests.integration.e2e.matrix import generate

generate(globals())

if __name__ == "__main__":
    unittest.main()
