"""The topology x scenario matrix and the generator that turns it into unittest cases.

A *topology* is an ordered list of ``ManagerSpec``s: one backend is a single-manager deployment, several are
a ``waterfall_v1`` deployment (priority = position), possibly cross-backend. Each topology lists the
scenarios it runs -- the framework lets any scenario run on any deployment, so this list is a curated matrix,
not a hard limit (``min_managers`` only guards against listing a multi-manager scenario on too small a
topology). Each topology becomes one generated class named for it (``Test_ecs``, ``Test_ecs_ec2``, ...),
gated by the same ``RUN_*_E2E`` flags the hand-written e2es used, so each on-demand workflow enables exactly
its own rows.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Callable, Dict, List

from scaler.utility.logging.utility import setup_logger
from tests.integration import (
    CONTAINER_E2E_SKIP_REASON,
    CROSS_BACKEND_E2E_SKIP_REASON,
    EC2_E2E_SKIP_REASON,
    FLOCI_E2E_SKIP_REASON,
    RUN_CONTAINER_E2E,
    RUN_CROSS_BACKEND_E2E,
    RUN_EC2_E2E,
    RUN_FLOCI_E2E,
)
from tests.integration._container_runtime import DockerRuntime
from tests.integration._ec2_backend import WHEEL_DIR, manylinux_wheel
from tests.integration._floci import floci_available
from tests.integration.e2e.backends import ContainerBackend, FlociEc2Backend, FlociEcsBackend
from tests.integration.e2e.framework import ManagerSpec, deploy
from tests.integration.e2e.scenarios import (
    burst_and_drain,
    rising_load,
    steady_load_stable,
    waterfall_spills,
    work_spreads,
)
from tests.utility.utility import logging_test_name

# A single manager exercises the scaling curve; a waterfall of managers exercises spill across tiers.
SCALING_SCENARIOS: List[Callable] = [burst_and_drain, work_spreads, rising_load, steady_load_stable]
WATERFALL_SCENARIOS: List[Callable] = [waterfall_spills]

_DOCKER = DockerRuntime.is_available()
_FLOCI = floci_available()
_WHEEL = bool(manylinux_wheel())
_NO_DOCKER = "Docker is required for this e2e"
_NO_WHEEL = f"no manylinux wheel under {WHEEL_DIR}; build one with scripts/build_cibuildwheel.sh"


@dataclasses.dataclass
class Topology:
    name: str
    specs: List[ManagerSpec]
    scenarios: List[Callable]
    enabled: bool
    skip_reason: str


def _floci_topology(
    name: str,
    specs: List[ManagerSpec],
    scenarios: List[Callable],
    run_flag: bool,
    off_reason: str,
    need_wheel: bool = False,
) -> Topology:
    if not run_flag:
        return Topology(name, specs, scenarios, False, off_reason)
    if not _FLOCI:
        return Topology(name, specs, scenarios, False, _NO_DOCKER)
    if need_wheel and not _WHEEL:
        return Topology(name, specs, scenarios, False, _NO_WHEEL)
    return Topology(name, specs, scenarios, True, "")


def _container_topology(
    name: str, specs: List[ManagerSpec], scenarios: List[Callable], run_flag: bool, off_reason: str
) -> Topology:
    if not run_flag:
        return Topology(name, specs, scenarios, False, off_reason)
    if not _DOCKER:
        return Topology(name, specs, scenarios, False, _NO_DOCKER)
    return Topology(name, specs, scenarios, True, "")


TOPOLOGIES: List[Topology] = [
    _floci_topology("ecs", [ManagerSpec(FlociEcsBackend())], SCALING_SCENARIOS, RUN_FLOCI_E2E, FLOCI_E2E_SKIP_REASON),
    _floci_topology(
        "ec2", [ManagerSpec(FlociEc2Backend())], SCALING_SCENARIOS, RUN_EC2_E2E, EC2_E2E_SKIP_REASON, need_wheel=True
    ),
    _container_topology(
        "container", [ManagerSpec(ContainerBackend())], SCALING_SCENARIOS, RUN_CONTAINER_E2E, CONTAINER_E2E_SKIP_REASON
    ),
    _floci_topology(
        "ecs_ec2",
        [ManagerSpec(FlociEcsBackend(), cap=2), ManagerSpec(FlociEc2Backend())],
        WATERFALL_SCENARIOS,
        RUN_CROSS_BACKEND_E2E,
        CROSS_BACKEND_E2E_SKIP_REASON,
        need_wheel=True,
    ),
    _container_topology(
        "container_waterfall",
        [ManagerSpec(ContainerBackend("scaler-it-a"), cap=2), ManagerSpec(ContainerBackend("scaler-it-b"))],
        WATERFALL_SCENARIOS,
        RUN_CONTAINER_E2E,
        CONTAINER_E2E_SKIP_REASON,
    ),
]


def _make_set_up(specs: List[ManagerSpec]) -> Callable:
    def set_up(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.deployment = deploy(self, specs)

    return set_up


def _tear_down(self) -> None:
    self.deployment.assert_healthy(self)


def _make_test(scenario: Callable) -> Callable:
    def test(self) -> None:
        scenario(self, self.deployment)

    return test


def generate(namespace: Dict) -> None:
    """Materialize one ``unittest.TestCase`` per topology into ``namespace`` (a test module's globals), with a
    ``test_<scenario>`` method per listed scenario. Raises if a topology lists a scenario needing more managers
    than it has -- a matrix wiring error, caught at import."""
    for topology in TOPOLOGIES:
        attributes: Dict[str, Callable] = {"setUp": _make_set_up(topology.specs), "tearDown": _tear_down}
        for scenario in topology.scenarios:
            if scenario.min_managers > len(topology.specs):  # type: ignore[attr-defined]
                raise ValueError(
                    f"topology {topology.name!r} has {len(topology.specs)} manager(s) but "
                    f"{scenario.__name__} needs {scenario.min_managers}"  # type: ignore[attr-defined]
                )
            attributes[f"test_{scenario.__name__}"] = _make_test(scenario)
        test_case = type(f"Test_{topology.name}", (unittest.TestCase,), attributes)
        namespace[test_case.__name__] = unittest.skipUnless(topology.enabled, topology.skip_reason)(test_case)
