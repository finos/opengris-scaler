"""End-to-end AWS integration tests (see README.md).

Opt-in: these tests are skipped by the default ``unittest discover`` run so they do
not add cloud-mock overhead to the standard local test run. Enable them with::

    RUN_INTEGRATION_TESTS=1 python -m unittest discover -s tests/integration -t . -v
"""

import os

RUN_INTEGRATION_TESTS = os.environ.get("RUN_INTEGRATION_TESTS") == "1"
INTEGRATION_SKIP_REASON = "set RUN_INTEGRATION_TESTS=1 to enable the end-to-end integration tests"
