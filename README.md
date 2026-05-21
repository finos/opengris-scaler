<div align="center">
  <a href="https://github.com/finos/opengris-scaler">
    <img src="https://github.com/finos/branding/blob/master/project-logos/active-project-logos/OpenGRIS/Scaler/2025_OpenGRIS_Scaler.svg" alt="OpenGRIS Scaler" width="180" height="80">
  </a>

  <p><strong>Efficient, lightweight, and reliable distributed computation.</strong></p>

  <p>
    <a href="https://community.finos.org/docs/governance/Software-Projects/stages/incubating"><img src="https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg" alt="FINOS Incubating"></a>
    <a href="https://finos.github.io/opengris-scaler/"><img src="https://img.shields.io/badge/Documentation-0f1632" alt="Documentation"></a>
    <a href="./LICENSE"><img src="https://img.shields.io/github/license/finos/opengris-scaler?label=license&colorA=0f1632&colorB=255be3" alt="License"></a>
    <a href="https://pypi.org/project/opengris-scaler"><img src="https://img.shields.io/pypi/v/opengris-scaler?colorA=0f1632&colorB=255be3" alt="PyPI"></a>
    <img src="https://api.securityscorecards.dev/projects/github.com/finos/opengris-scaler/badge" alt="OpenSSF Scorecard">
  </p>
</div>

> **Documentation:** https://finos.github.io/opengris-scaler/
>
> Start there for installation options, command reference, worker manager guides, scaling policies, and examples.

## What Is Scaler?

OpenGRIS Scaler is a distributed computing framework for running Python tasks across local machines or remote infrastructure.

It provides:

- A Python client API similar to `multiprocessing` patterns such as `submit()`, `map()`, and `starmap()`.
- A centralized scheduler that dispatches work and balances load across workers.
- Worker managers for local execution and cloud-backed capacity.
- Support for graph/DAG execution, monitoring, and task recovery.

## Architecture

![Scaler architecture](docs/source/tutorials/images/architecture.svg)

- Clients submit tasks to a scheduler.
- The scheduler tracks state, applies scaling/allocation policies, and dispatches work.
- Worker managers provision workers locally or on external infrastructure.
- Workers execute tasks and return results.
- An object storage service stores task inputs and outputs used by the cluster.

## Local Quickstart

Install the package:

```bash
pip install opengris-scaler
```

Create `config.toml`:

```toml
[object_storage_server]
bind_address = "tcp://127.0.0.1:8517"

[scheduler]
bind_address = "tcp://127.0.0.1:8516"
object_storage_address = "tcp://127.0.0.1:8517"

[[worker_manager]]
type = "baremetal_native"
scheduler_address = "tcp://127.0.0.1:8516"
worker_manager_id = "wm-native"
```

Start a fully local stack:

```bash
scaler config.toml
```

Submit tasks from Python:

```python
from scaler import Client


def square(value: int) -> int:
    return value * value


with Client(address="tcp://127.0.0.1:8516") as client:
    results = client.map(square, range(10))

print(results)
```

## Learn More

- Docs home: https://finos.github.io/opengris-scaler/
- Quickstart: https://finos.github.io/opengris-scaler/tutorials/quickstart.html
- Overview: https://finos.github.io/opengris-scaler/tutorials/overview.html
- Commands: https://finos.github.io/opengris-scaler/tutorials/commands.html
