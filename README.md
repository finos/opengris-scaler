<div align="center">
  <a href="https://github.com/citi">
    <img src="https://github.com/citi.png" alt="Citi" width="80" height="80">
  </a>

<h3 align="center">Citi/scaler</h3>

  <p align="center">
    Efficient, lightweight and reliable distributed computation engine.
  </p>

  <p align="center">
    <a href="./LICENSE">
        <img src="https://img.shields.io/github/license/citi/scaler?label=license&colorA=0f1632&colorB=255be3">
    </a>
    <a href="https://pypi.org/project/scaler">
      <img alt="PyPI - Version" src="https://img.shields.io/pypi/v/scaler?colorA=0f1632&colorB=255be3">
    </a>
    <img src="https://api.securityscorecards.dev/projects/github.com/Citi/scaler/badge">
  </p>
</div>

<br />

**Scaler provides a simple, efficient and reliable way to perform distributed computing** using a centralized scheduler,
with a stable and language agnostic protocol for client and worker communications.

```python
import math
from scaler import Client

with Client(address="tcp://127.0.0.1:2345") as client:
    # Submits 100 tasks
    futures = [
        client.submit(math.sqrt, i)
        for i in range(0, 100)
    ]

    # Collects the results and sums them
    result = sum(future.result() for future in futures)

    print(result)  # 661.46
```

Scaler is a suitable Dask replacement, offering significantly better scheduling performance for jobs with a large number
of lightweight tasks while improving on load balancing, messaging and deadlocks.

## Features

- Distributed computing on **multiple cores and multiple servers**
- **Python** reference implementation, with **language agnostic messaging protocol** built on top of
  [Cap'n Proto](https://capnproto.org/) and [ZeroMQ](https://zeromq.org)
- **Graph** scheduling, which supports [Dask](https://www.dask.org)-like graph computing, optionally you
  can use [GraphBLAS](https://graphblas.org) for very large graph tasks
- **Automated load balancing**. automatically balances load from busy workers to idle workers and tries to keep workers
  utilized as uniformly as possible
- **Automated task recovery** from faulting workers who have died
- Supports for **nested tasks**, tasks can themselves submit new tasks
- `top`-like **monitoring tools**
- GUI monitoring tool

Scaler's scheduler can be run on PyPy, which can provide a performance boost

## Installation

```bash
$ pip install scaler

# or with graphblas and uvloop support
$ pip install scaler[graphblas,uvloop]
```

## Quick Start

Scaler operates around 3 components:

- A **scheduler**, responsible for routing tasks to available computing resources
- A set of **workers**, or cluster. Workers are independent computing units, each capable of executing a single task
- **Clients** running inside applications, responsible for submitting tasks to the scheduler.

### Start local scheduler and cluster at the same time in code

A local scheduler and a local set of workers can be conveniently spawn using `SchedulerClusterCombo`:

```python
from scaler import SchedulerClusterCombo

cluster = SchedulerClusterCombo(address="tcp://127.0.0.1:2345", n_workers=4)

...

cluster.shutdown()
```

This will start a scheduler with 4 task executing workers on port `2345`.

### Setting up a computing cluster from the CLI

The scheduler and workers can also be started from the command line with `scaler_scheduler` and `scaler_cluster`.

First start the Scaler scheduler:

```bash
$ scaler_scheduler tcp://127.0.0.1:2345
[INFO]2023-03-19 12:16:10-0400: logging to ('/dev/stdout',)
[INFO]2023-03-19 12:16:10-0400: use event loop: 2
[INFO]2023-03-19 12:16:10-0400: Scheduler: monitor address is ipc:///tmp/127.0.0.1_2345_monitor
...
```

Then start a set of workers (a.k.a. a Scaler *cluster*) that connect to the previously started scheduler:

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

`-h` lists the available options for the scheduler and the cluster executables:

```bash
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
    future = client.submit(square, 4)
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
    "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
    "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
    "e": (minus, "d", "c")  # e = d - c = 4 - 3 = 1
}

with Client(address="tcp://127.0.0.1:2345") as client:
    result = client.get(graph, keys=["e"])
    print(result)  # {"e": 1}
```

## Nested computations

Scaler allows tasks to submit new tasks while being executed. Scaler also supports recursive task calls.

```python
from scaler import Client


def fibonacci(clnt: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = clnt.submit(fibonacci, clnt, n - 1)
        b = clnt.submit(fibonacci, clnt, n - 2)
        return a.result() + b.result()


with Client(address="tcp://127.0.0.1:2345") as client:
    result = client.submit(fibonacci, client, 8).result()
    print(result)  # 21
```

## Performance

### uvloop

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
$ scaler_top ipc:///tmp/127.0.0.1_2345_monitor
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

- scheduler section shows scheduler resource usage
- task_manager section shows count for each task status
- scheduler_sent section shows count for each type of messages scheduler sent
- scheduler_received section shows count for each type of messages scheduler received
- function_id_to_tasks section shows task count for each function used
- worker section shows worker details, you can use shortcuts to sort by columns, the char * on column header show which
  column is sorted right now
    - agt_cpu/agt_rss means cpu/memory usage of worker agent
    - cpu/rss means cpu/memory usage of worker
    - free means number of free task slots for this worker
    - sent means how many tasks scheduler sent to the worker
    - queued means how many tasks worker received and queued

### From the web UI

`scaler_ui` provides a web monitoring interface for Scaler.

```bash
$ scaler_ui ipc:///tmp/127.0.0.1_2345_monitor --port 8081
```

This will open a web server on port `8081`.

## Contributing

Your contributions are at the core of making this a true open source project. Any contributions you make are **greatly
appreciated**.

We welcome you to:

- Fix typos or touch up documentation
- Share your opinions on [existing issues](https://github.com/citi/scaler/issues)
- Help expand and improve our library by [opening a new issue](https://github.com/citi/scaler/issues/new)

Please review our [community contribution guidelines](https://github.com/Citi/.github/blob/main/CONTRIBUTING.md) and
[functional contribution guidelines](./CONTRIBUTING.md) to get started 👍.

## Code of Conduct

We are committed to making open source an enjoyable and respectful experience for our community. See
[`CODE_OF_CONDUCT`](https://github.com/Citi/.github/blob/main/CODE_OF_CONDUCT.md) for more information.

## License

This project is distributed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0). See
[`LICENSE`](./LICENSE) for more information.

## Contact

If you have a query or require support with this project, [raise an issue](https://github.com/Citi/scaler/issues).
Otherwise, reach out to [opensource@citi.com](mailto:opensource@citi.com).
