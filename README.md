<div align="center">
  <a href="https://github.com/finos/opengris-scaler">
    <img src="https://github.com/finos/branding/blob/master/project-logos/active-project-logos/OpenGRIS/Scaler/2025_OpenGRIS_Scaler.svg" alt="OpenGRIS Scaler" width="180" height="80">
  </a>

  <p align="center">
    Efficient, lightweight, and reliable distributed computation engine.
  </p>

  <p align="center">
    <a href="https://community.finos.org/docs/governance/Software-Projects/stages/incubating">
      <img src="https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg">
    </a>
    <a href="https://finos.github.io/opengris-scaler/">
      <img src="https://img.shields.io/badge/Documentation-0f1632">
    </a>
    <a href="./LICENSE">
        <img src="https://img.shields.io/github/license/finos/opengris-scaler?label=license&colorA=0f1632&colorB=255be3">
    </a>
    <a href="https://pypi.org/project/opengris-scaler">
      <img alt="PyPI - Version" src="https://img.shields.io/pypi/v/opengris-scaler?colorA=0f1632&colorB=255be3">
    </a>
    <img src="https://api.securityscorecards.dev/projects/github.com/finos/opengris-scaler/badge">
  </p>
</div>

<br />

**OpenGRIS Scaler provides a simple, efficient, and reliable way to perform distributed computing** using a centralized
scheduler,
with a stable and language-agnostic protocol for client and worker communications.

```python
import math
from scaler import Client

with Client(address="tcp://127.0.0.1:2345") as client:
    # Compute a single task using `.submit()`
    future = client.submit(math.sqrt, 16)
    print(future.result())  # 4

    # Submit multiple tasks with `.map()` - works like Python's built-in map()
    results = client.map(math.sqrt, range(100))
    print(sum(results))  # 661.46

    # For functions with multiple arguments, use multiple iterables or `.starmap()`
    def add(x, y):
        return x + y

    client.map(add, [1, 2, 3], [10, 20, 30])  # [11, 22, 33]
    client.starmap(add, [(1, 10), (2, 20), (3, 30)])  # [11, 22, 33]
```

OpenGRIS Scaler is a suitable Dask replacement, offering significantly better scheduling performance for jobs with a
large number
of lightweight tasks while improving on load balancing, messaging, and deadlocks.

## Features

- Distributed computing across **multiple cores and multiple servers**
- **Python** reference implementation, with **language-agnostic messaging protocol** built on top of
  [Cap'n Proto](https://capnproto.org/) and [ZeroMQ](https://zeromq.org)
- **Graph** scheduling, which supports [Dask](https://www.dask.org)-like graph computing, with
  optional [GraphBLAS](https://graphblas.org)
  support for very large graph tasks
- **Automated load balancing**, which automatically balances load from busy workers to idle workers, ensuring uniform
  utilization across workers
- **Automated task recovery** from worker-related hardware, OS, or network failures
- Support for **nested tasks**, allowing tasks to submit new tasks
- `top`-like **monitoring tools**
- GUI monitoring tool

## Installation

Scaler is available on PyPI and can be installed using any compatible package manager.

```bash
$ pip install opengris-scaler

# or with graphblas and uvloop and webui support
$ pip install opengris-scaler[graphblas,uvloop,gui]

# or simply
$ pip install opengris-scaler[all]
```

## Quick Start

The official documentation is available at [finos.github.io/opengris-scaler/](https://finos.github.io/opengris-scaler/).

Scaler has 3 main components:

- A **scheduler**, responsible for routing tasks to available computing resources.
- An **object storage server** that stores the task data objects (task arguments and task results).
- A set of **workers** that form a _cluster_. Workers are independent computing units, each capable of executing a
  single task.
- **Clients** running inside applications, responsible for submitting tasks to the scheduler.

Please be noted that **Clients** are cross platform, supporting Windows and GNU/Linux, while other components can only
be run on GNU/Linux.

### Start local scheduler and cluster programmatically in code

A local scheduler and a local set of workers can be conveniently started using `SchedulerClusterCombo`:

```python
from scaler import SchedulerClusterCombo

cluster = SchedulerClusterCombo(address="tcp://127.0.0.1:2345", n_workers=4)

...

cluster.shutdown()
```

This will start a scheduler with 4 workers on port `2345`.

### Setting up a computing cluster from the CLI

The object storage server, scheduler and workers can also be started from the command line with
`scaler_scheduler` and `scaler_cluster`.

First, start the scheduler, and make it connect to the object storage server:

```bash
$ scaler_scheduler "tcp://127.0.0.1:2345"
[INFO]2025-06-06 13:13:15+0200: logging to ('/dev/stdout',)
[INFO]2025-06-06 13:13:15+0200: use event loop: builtin
[INFO]2025-06-06 13:13:15+0200: Scheduler: listen to scheduler address tcp://127.0.0.1:2345
[INFO]2025-06-06 13:13:15+0200: Scheduler: connect to object storage server tcp://127.0.0.1:2346
[INFO]2025-06-06 13:13:15+0200: Scheduler: listen to scheduler monitor address tcp://127.0.0.1:2347
...
```

Finally, start a set of workers (a.k.a. a Scaler *cluster*) that connects to the previously started scheduler:

```bash
$ scaler_cluster -n 4 tcp://127.0.0.1:2345
[INFO]2023-03-19 12:19:19-0400: logging to ('/dev/stdout',)
[INFO]2023-03-19 12:19:19-0400: ClusterProcess: starting 4 workers, heartbeat_interval_seconds=2, object_retention_seconds=3600
[INFO]2023-03-19 12:19:19-0400: Worker[0] started
[INFO]2023-03-19 12:19:19-0400: Worker[1] started
[INFO]2023-03-19 12:19:19-0400: Worker[2] started
[INFO]2023-03-19 12:19:19-0400: Worker[3] started
...
```

Multiple Scaler clusters can be connected to the same scheduler, providing distributed computation over multiple
servers.

`-h` lists the available options for the object storage server, scheduler and the cluster executables:

```bash
$ scaler_object_storage_server -h
$ scaler_scheduler -h
$ scaler_cluster -h
```

### Submitting Python tasks using the Scaler client

Knowing the scheduler address, you can connect and submit tasks from a client in your Python code:

```python
from scaler import Client


def square(value: int):
    return value * value


with Client(address="tcp://127.0.0.1:2345") as client:
    future = client.submit(square, 4)  # submits a single task
    print(future.result())  # 16
```

`Client.submit()` returns a standard Python future.

## Graph computations

Scaler also supports graph tasks, for example:

```python
from scaler import Client


def inc(i):
    return i + 1


def add(a, b):
    return a + b


def minus(a, b):
    return a - b


graph = {
    "a": 2,
    "b": 2,

    # the input to task c is the output of task a
    "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
    "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
    "e": (minus, "d", "c")  # e = d - c = 4 - 3 = 1
}

with Client(address="tcp://127.0.0.1:2345") as client:
    result = client.get(graph, keys=["e"])
    print(result)  # {"e": 1}
```

## Configuring with TOML Files

While all Scaler components can be configured using command-line flags, using TOML files is the recommended approach for
production or shareable setups. Configuration files make your setup explicit, easier to manage, and allow you to check
your infrastructure's configuration into version control.

For convenience, you can define the settings for all components in a single, sectioned TOML file. Each component
automatically loads its configuration from its corresponding section.

### Core Concepts

* **Usage**: To use a configuration file, pass its path via the `--config` or `-c` flag.

    ```bash
    scaler_scheduler --config /path/to/your/example_config.toml
    ```

* **Precedence**: Settings are loaded in a specific order, with later sources overriding earlier ones. The hierarchy is:

  Command-Line Flags > TOML File Settings > Built-in Default Values

* **Naming Convention**: The keys in the TOML file must match the long-form command-line arguments. The rule is to
  replace any hyphens (`-`) with underscores (`_`).
    * For example, the flag `--num-of-workers` becomes the TOML key `num_of_workers`.
    * One can discover all available keys by running any command with the `-h` or `--help` flag.

### Supported Components and Section Names

The following table maps each Scaler command to its corresponding section name in the TOML file.

| Command                          | TOML Section Name           |
|----------------------------------|-----------------------------|
| `scaler_scheduler`               | `[scheduler]`               |
| `scaler_cluster`                 | `[cluster]`                 |
| `scaler_object_storage_server`   | `[object_storage_server]`   |
| `scaler_ui`                      | `[webui]`                   |
| `scaler_top`                     | `[top]`                     |
| `scaler_worker_adapter_native`   | `[native_worker_adapter]`   |
| `scaler_worker_adapter_symphony` | `[symphony_worker_adapter]` |

### Practical Scenarios & Examples

#### Scenario 1: Unified Configuration File

Here is an example of a single `example_config.toml` file that configures multiple components using sections.

**example_config.toml**

```toml
# This is a unified configuration file for all Scaler components.

[scheduler]
object_storage_address = "tcp://127.0.0.1:6379"
monitor_address = "tcp://127.0.0.1:6380"
allocate_policy = "even"
logging_level = "INFO"
logging_paths = ["/dev/stdout", "/var/log/scaler/scheduler.log"]

[cluster]
num_of_workers = 8
per_worker_capabilities = "linux,cpu=8"
task_timeout_seconds = 600

[object_storage_server]

[webui]
web_port = 8081
```

With this single file, starting your entire stack is simple and consistent:

```bash
scaler_object_storage_server tcp://127.0.0.1:6379 --config example_config.toml &
scaler_scheduler tcp://127.0.0.1:6378 --config example_config.toml &
scaler_cluster tcp://127.0.0.1:6378 --config example_config.toml &
scaler_ui tcp://127.0.0.1:6380 --config example_config.toml &
```

#### Scenario 2: Overriding a Section's Setting

You can override any value from the TOML file by providing it as a command-line flag. For example, to use the
example_config.toml file but test the cluster with 12 workers instead of 8:

```bash
# The --num-of-workers flag will take precedence over the [cluster] section
scaler_cluster tcp://127.0.0.1:6378 --config example_config.toml --num-of-workers 12
```

The cluster will start with 12 workers, but all other settings (like `task_timeout_seconds`) will still be loaded from the
`[cluster]` section of example_config.toml.

## Nested computations

Scaler allows tasks to submit new tasks while being executed. Scaler also supports recursive task calls.

```python
from scaler import Client


def fibonacci(client: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = client.submit(fibonacci, client, n - 1)
        b = client.submit(fibonacci, client, n - 2)
        return a.result() + b.result()


with Client(address="tcp://127.0.0.1:2345") as client:
    future = client.submit(fibonacci, client, 8)
    print(future.result())  # 21
```

**Note**: When creating a `Client` inside a task (nested client), the `address` parameter is optional. If omitted, the client automatically uses the scheduler address from the worker context. If provided, the specified address takes precedence.

## Task Routing and Capability Management

> **Note**: This feature is experimental and may change in future releases.

Scaler provides a task routing mechanism, allowing you to specify capability requirements for tasks and allocate them to
workers supporting these.

### Starting the Scheduler with the Capability Allocation Policy

The scheduler can be started with the experimental capability allocation policy using the `--allocate-policy/-ap`
argument.

```bash
$ scaler_scheduler --allocate-policy capability tcp://127.0.0.1:2345
```

### Defining Worker Supported Capabilities

When starting a cluster of workers, you can define the capabilities available on each worker using the
`--per-worker-capabilities/-pwc` argument. This allows the scheduler to allocate tasks to workers based on the
capabilities these provide.

```bash
$ scaler_cluster -n 4 --per-worker-capabilities "gpu,linux" tcp://127.0.0.1:2345
```

### Specifying Capability Requirements for Tasks

When submitting tasks using the Scaler client, you can specify the capability requirements for each task using the
`capabilities` argument in the `submit_verbose()` and `get()` methods. This ensures that tasks are allocated to workers
supporting these capabilities.

```python
from scaler import Client

with Client(address="tcp://127.0.0.1:2345") as client:
    future = client.submit_verbose(round, args=(3.15,), kwargs={}, capabilities={"gpu": -1})
    print(future.result())  # 3
```

The scheduler will route a task to a worker if `task.capabilities.is_subset(worker.capabilities)`.

Integer values specified for capabilities (e.g., `gpu=10`) are *currently* ignored by the capabilities allocation
policy.
This means that the presence of a capabilities is considered, but not its quantity. Support for capabilities tracking
might be added in the future.

## IBM Spectrum Symphony integration

A Scaler scheduler can interface with IBM Spectrum Symphony to provide distributed computing across Symphony clusters.

```bash
$ scaler_worker_adapter_symphony tcp://127.0.0.1:2345 --service-name ScalerService --base-concurrency 4 --host 127.0.0.1 --port 8080
```

This will start a Scaler worker that connects to the Scaler scheduler at `tcp://127.0.0.1:2345` and uses the Symphony
service `ScalerService` to submit tasks.

### Symphony service

A service must be deployed in Symphony to handle the task submission.

<details>

<summary>Here is an example of a service that can be used</summary>

```python
class Message(soamapi.Message):
    def __init__(self, payload: bytes = b""):
        self.__payload = payload

    def set_payload(self, payload: bytes):
        self.__payload = payload

    def get_payload(self) -> bytes:
        return self.__payload

    def on_serialize(self, stream):
        payload_array = array.array("b", self.get_payload())
        stream.write_byte_array(payload_array, 0, len(payload_array))

    def on_deserialize(self, stream):
        self.set_payload(stream.read_byte_array("b"))


class ServiceContainer(soamapi.ServiceContainer):
    def on_create_service(self, service_context):
        return

    def on_session_enter(self, session_context):
        return

    def on_invoke(self, task_context):
        input_message = Message()
        task_context.populate_task_input(input_message)

        fn, *args = cloudpickle.loads(input_message.get_payload())
        output_payload = cloudpickle.dumps(fn(*args))

        output_message = Message(output_payload)
        task_context.set_task_output(output_message)

    def on_session_leave(self):
        return

    def on_destroy_service(self):
        return
```

</details>

### Nested tasks

Nested task originating from Symphony workers must be able to reach the Scaler scheduler. This might require
modifications to the network configuration.

Nested tasks can also have unpredictable resource usage and runtimes, which can cause Symphony to prematurely kill
tasks. It is recommended to be conservative when provisioning resources and limits, and monitor the cluster status
closely for any abnormalities.

### Base concurrency

Base concurrency is the maximum number of unnested tasks that can be executed concurrently. It is possible to surpass
this limit by submitting nested tasks which carry a higher priority. **Important**: If your workload contains nested
tasks the base concurrency should be set to a value less to the number of cores available on the Symphony worker or else
deadlocks may occur.

A good heuristic for setting the base concurrency is to use the following formula:

```
base_concurrency = number_of_cores - deepest_nesting_level
```

where `deepest_nesting_level` is the deepest nesting level a task has in your workload. For instance, if you have a
workload that has
a base task that calls a nested task that calls another nested task, then the deepest nesting level is 2.

## Worker Adapter usage

> **Note**: This feature is experimental and may change in future releases.

Scaler provides a Worker Adapter webhook interface to integrate with other job schedulers or resource managers. The
Worker Adapter allows external systems to request the creation and termination of Scaler workers dynamically.

Please check the OpenGRIS standard for more details on the Worker Adapter
specification [here](https://github.com/finos/opengris).

### Starting the Native Worker Adapter

Starting a Native Worker Adapter server at `http://127.0.0.1:8080`:

```bash
$ scaler_worker_adapter_native tcp://127.0.0.1:2345 --host 127.0.0.1 --port 8080
```

Pass the `--adapter-webhook-url` argument to the Scaler scheduler to connect to the Worker Adapter:

```bash
$ scaler_scheduler tcp://127.0.0.1:2345 --adapter-webhook-url http://127.0.0.1:8080
````

To check that the Worker Adapter is working, you can bring up `scaler_top` to see workers spawning and terminating as
there is task load changes.

## Performance

### uvloop

By default, Scaler uses Python's built-in `asyncio` event loop.
For better async performance, you can install uvloop (`pip install uvloop`) and supply `uvloop` for the CLI argument
`--event-loop` or as a keyword argument for `event_loop` in Python code when initializing the scheduler.

```bash
scaler_scheduler --event-loop uvloop tcp://127.0.0.1:2345
```

```python
from scaler import SchedulerClusterCombo

scheduler = SchedulerClusterCombo(address="tcp://127.0.0.1:2345", event_loop="uvloop", n_workers=4)
```

## Monitoring

### From the CLI

Use `scaler_top` to connect to the scheduler's monitor address (printed by the scheduler on startup) to see
diagnostics/metrics information about the scheduler and its workers.

```bash
$ scaler_top tcp://127.0.0.1:2347
```

It will look similar to `top`, but provides information about the current Scaler setup:

```bash
scheduler          | task_manager         |   scheduler_sent         | scheduler_received
      cpu     0.0% |   unassigned       0 |   ObjectResponse      24 |          Heartbeat 183,109
      rss 37.1 MiB |      running       0 |         TaskEcho 200,000 |    ObjectRequest      24
                   |      success 200,000 |             Task 200,000 |               Task 200,000
                   |       failed       0 |       TaskResult 200,000 |         TaskResult 200,000
                   |     canceled       0 |   BalanceRequest       4 |    BalanceResponse       4
--------------------------------------------------------------------------------------------------
Shortcuts: worker[n] cpu[c] rss[m] free[f] working[w] queued[q]

Total 10 worker(s)
                 worker agt_cpu agt_rss [cpu]   rss free sent queued | object_id_to_tasks
W|Linux|15940|3c9409c0+    0.0%   32.7m  0.0% 28.4m 1000    0      0 |
W|Linux|15946|d6450641+    0.0%   30.7m  0.0% 28.2m 1000    0      0 |
W|Linux|15942|3ed56e89+    0.0%   34.8m  0.0% 30.4m 1000    0      0 |
W|Linux|15944|6e7d5b99+    0.0%   30.8m  0.0% 28.2m 1000    0      0 |
W|Linux|15945|33106447+    0.0%   31.1m  0.0% 28.1m 1000    0      0 |
W|Linux|15937|b031ce9a+    0.0%   31.0m  0.0% 30.3m 1000    0      0 |
W|Linux|15941|c4dcc2f3+    0.0%   30.5m  0.0% 28.2m 1000    0      0 |
W|Linux|15939|e1ab4340+    0.0%   31.0m  0.0% 28.1m 1000    0      0 |
W|Linux|15938|ed582770+    0.0%   31.1m  0.0% 28.1m 1000    0      0 |
W|Linux|15943|a7fe8b5e+    0.0%   30.7m  0.0% 28.3m 1000    0      0 |
```

- **scheduler** section shows scheduler resource usage
- **task_manager** section shows count for each task status
- **scheduler_sent** section shows count for each type of messages scheduler sent
- **scheduler_received** section shows count for each type of messages scheduler received
- **function_id_to_tasks** section shows task count for each function used
- **worker** section shows worker details, , you can use shortcuts to sort by columns, and the * in the column header
  shows
  which column is being used for sorting
    - `agt_cpu/agt_rss` means cpu/memory usage of worker agent
    - `cpu/rss` means cpu/memory usage of worker
    - `free` means number of free task slots for this worker
    - `sent` means how many tasks scheduler sent to the worker
    - `queued` means how many tasks worker received and queued

### From the web UI

`scaler_ui` provides a web monitoring interface for Scaler.

```bash
$ scaler_ui tcp://127.0.0.1:2347 --port 8081
```

This will open a web server on port `8081`.

## Slides and presentations

We showcased Scaler at FOSDEM 2025. Check out the slides
[here](<slides/Effortless Distributed Computing in Python - FOSDEM 2025.pdf>).

## Building from source

### Using the Dev Container (Recommended)

The easiest way to build Scaler is by using the provided dev container.
See the [Dev Container Setup documentation](https://finos.github.io/opengris-scaler/tutorials/development/devcontainer.html) for more details.

### Building on GNU/Linux

To contribute to Scaler, you might need to manually build its C++ components.

These C++ components depend on the Boost and Cap'n Proto libraries. If these libraries are not available on your system,
you can use the `library_tool.sh` script to download, compile, and install them (You might need `sudo`):

```bash
./scripts/library_tool.sh boost compile
./scripts/library_tool.sh boost install
./scripts/library_tool.sh capnp compile
./scripts/library_tool.sh capnp install
```

After installing these dependencies, use the `build.sh` script to configure, build, and install Scaler's C++ components:

```bash
./scripts/build.sh
```

This script will create a build directory based on your operating system and architecture, and install the components
within the main source tree, as compiled Python modules. You can specify the compiler to use by setting the `CC` and
`CXX` environment variables.

### Building on Windows

*Building on Windows requires _Visual Studio 17 2022_*. Similar to the former section, you can use the
`library_tool.ps1` script to download, compile, and install them (You might need `Run as administrator`):

```bash
./scripts/library_tool.ps1 boost compile
./scripts/library_tool.ps1 boost install
./scripts/library_tool.ps1 capnp compile
./scripts/library_tool.ps1 capnp install
```

After installing these dependencies, if you are using _Visual Studio_ for developing, you may open the project folder
with it, select preset `windows-x64`, and build the project. You may also run the following commands to configure,
build, and install Scaler's C++ components:

```bash
cmake --preset windows-x64
cmake --build --preset windows-x64 --config (Debug|Release)
cmake --install build_windows_x64 --config (Debug|Release)
```

The output will be similar to what described in the former section. We recommend using _Visual Studio_ for developing on
Windows.

### Building the Python wheel

Build the Python wheel for Scaler using `cibuildwheel`:

```bash
pip install build cibuildwheel

python -m cibuildwheel --output-dir wheelhouse
python -m build --sdist
```

## Contributing

Your contributions are at the core of making this a true open source project. Any contributions you make are **greatly
appreciated**.

We welcome you to:

- Fix typos or touch up documentation
- Share your opinions on [existing issues](https://github.com/finos/opengris-scaler/issues)
- Help expand and improve our library by [opening a new issue](https://github.com/finos/opengris-scaler/issues/new)

Please review [functional contribution guidelines](./CONTRIBUTING.md) to get started 👍.

_NOTE:_ Commits and pull requests to FINOS repositories will only be accepted from those contributors with an active,
executed Individual Contributor License Agreement (ICLA) with FINOS OR contributors who are covered under an existing
and active Corporate Contribution License Agreement (CCLA) executed with FINOS. Commits from individuals not covered
under an ICLA or CCLA will be flagged and blocked by
the ([EasyCLA](https://community.finos.org/docs/governance/Software-Projects/easycla)) tool. Please note that some CCLAs
require individuals/employees to be explicitly named on the CCLA.

*Need an ICLA? Unsure if you are covered under an existing CCLA? Email [help@finos.org](mailto:help@finos.org)*

## Code of Conduct

Please see the FINOS [Community Code of Conduct](https://www.finos.org/code-of-conduct).

## License

Copyright 2023 Citigroup, Inc.

This project is distributed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0). See
[`LICENSE`](./LICENSE) for more information.

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)

## Contact

If you have a query or require support with this
project, [raise an issue](https://github.com/finos/opengris-scaler/issues).
Otherwise, reach out to [opensource@citi.com](mailto:opensource@citi.com).
