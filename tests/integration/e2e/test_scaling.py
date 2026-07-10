"""Generated backend x scenario scaling e2es. The cases (``Test_ecs``, ``Test_ec2``, ``Test_container``,
``Test_ecs_ec2``, ``Test_container_waterfall``) are materialized from the matrix in e2e/matrix.py; each is
gated by its ``RUN_*_E2E`` flag, so an on-demand workflow runs exactly its own topology and skips the rest.
"""

from __future__ import annotations

import unittest

from tests.integration.e2e.matrix import generate

generate(globals())

if __name__ == "__main__":
    unittest.main()
