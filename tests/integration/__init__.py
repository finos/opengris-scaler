"""End-to-end integration test skeleton (see README.md).

Opt-in: these full-stack tests are skipped by the default ``unittest discover`` run so they do
not add process-spawning / cloud-mock overhead to the standard CI build. Enable them with::

    RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v
"""

import os

RUN_INTEGRATION_TESTS = os.environ.get("RUN_INTEGRATION_TESTS") == "1"
INTEGRATION_SKIP_REASON = "set RUN_INTEGRATION_TESTS=1 to enable the end-to-end integration tests"

# A separate, heavier gate for the scaling stress test (spawns many processes). Kept out of the
# default integration run so it only executes when explicitly requested (e.g. the manually-triggered
# CI workflow), independent of RUN_INTEGRATION_TESTS.
RUN_SCALING_STRESS_TEST = os.environ.get("RUN_SCALING_STRESS_TEST") == "1"
SCALING_STRESS_SKIP_REASON = "set RUN_SCALING_STRESS_TEST=1 to enable the scaling stress test"
