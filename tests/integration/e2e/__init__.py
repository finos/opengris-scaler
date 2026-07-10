"""A backend-agnostic e2e scaling framework (prototype).

A *scenario* (e2e/scenarios.py) describes a client workload plus an assertion on how the pool scales; a
*worker-manager backend* (e2e/backends.py) supplies how one manager is provisioned and how its pool of
Docker containers is observed. A *Deployment* (e2e/framework.py) binds a scheduler harness to one or more
managers at waterfall priorities, so the same scenario runs against any single backend or any combination
(including cross-backend). The wiring that turns backend x scenario into unittest cases lives in
e2e/test_scaling.py.

This exists to stop the four hand-written e2e modules from drifting: the scenario bodies are written once
here and every backend reuses them.
"""
