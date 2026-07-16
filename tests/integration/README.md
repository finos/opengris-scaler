# End-to-end scaling tests

Do real workers provision, connect, scale, and return correct results? A real client bursts work at a real
scheduler; its scaling policy drives a real worker manager, which boots real workers in real Docker
containers that run the tasks.

Two of the three backends are the **shipped** AWS worker managers, unmodified: only their boto3 endpoint
and a couple of config values differ from a production deploy.

## Running one

Each needs a Docker daemon, builds images, and takes minutes, so they are opt-in and one runs per
invocation:

```bash
./scripts/e2e/run.sh container                 # container_waterfall | ecs | ec2 | ecs_ec2
DOCKER="sudo docker" ./scripts/e2e/run.sh ecs  # if the docker socket is root-only
```

The script builds any missing wheel/image, then runs
`SCALER_E2E=<topology> python -m unittest tests.integration.test_scaling -v`. It prints a
`web GUI: http://localhost:PORT` line you can open to watch the pool scale live.

In CI: the **E2E Scaling Tests** workflow, from the Actions tab (pick a topology) or by adding an
`e2e-<topology>` label to a PR.

Every scenario prints its numbers on an `[e2e]` line, so `grep '\[e2e\]'` over a run's log is the summary
of what it proved. CI does exactly that, and fails a run that produced none.

## Topologies

| `SCALER_E2E` | Manager(s) | What is real | What stands in for the cloud |
|---|---|---|---|
| `container` | one container manager | client, scheduler, workers in containers with their own IPs | nothing: a container *is* the provisioned machine |
| `container_waterfall` | two, at priorities 1 (capped) and 2 | as above, plus the scheduler's `waterfall_v1` spill | as above |
| `ecs` | the **shipped ECS worker manager** | its boto3 calls, plus workers in real ECS task containers | [floci](https://github.com/floci-io/floci), which really launches `RunTask` containers |
| `ec2` | the **shipped ORB/EC2 worker manager** | the ORB SDK, its shipped UserData, plus workers on real Amazon Linux 2023 instances that install a current-source wheel and boot | floci, launching `RunInstances` containers |
| `ecs_ec2` | **both** shipped AWS managers, one scheduler | ECS tasks and EC2 instances running work together, spill and all | floci, driving both |

floci is a free local AWS emulator that -- unlike moto or the community LocalStack -- actually launches the
containers it is asked to. That is what lets the shipped managers run a real scale curve, with real
workers, without an AWS account.

## Scenarios

Backend-agnostic, in `scenarios.py`; each topology lists the ones it runs in `test_scaling.py`.

* `burst_and_drain` -- a held burst brings the pool up and returns correct results; going idle drains it
  back to zero.
* `rising_load` -- a sustained deep burst runs more units **at once** than a concurrency-1 trickle.
* `steady_load_stable` -- a steady load settles on a stable pool (units created over the run stays close to
  peak units running at once) rather than thrashing through provision and teardown.
* `waterfall_spills` -- (needs >= 2 managers) a replenished backlog fills the capped top pool, then spills
  to the next tier. Each tier is attributed by its own container pool.

## Layout

| File | |
|---|---|
| `test_scaling.py` | one class per topology: the whole matrix, readable top to bottom |
| `scenarios.py` | what is asserted, written once and reused by every backend |
| `backends.py` | the three backends, each with the entry point that runs its manager in its own process |
| `framework.py` | `Profile`, `Deployment`, pool observation, and the `deploy()` that composes them |
| `harness.py` | the real scheduler + object storage processes |
| `docker.py`, `floci.py` | the container CLI, the images, and the emulator |

Two unifications let one scenario drive every backend: they all run their workers in host Docker
containers, so observing a pool is one prefix-scoped `docker ps`; and tasks are cloudpickled **by value**
and tag their unit with `SCALER_IT_MACHINE_ID or hostname`, covering both the container backend's
per-machine env id and the AWS managers (which set no id) in one expression.

`Deployment.assert_healthy` runs in every `tearDown`: if the scheduler or a manager crashes -- or stays up
but wedges the pool -- the test fails **naming the fault**, read from a tee'd scheduler log, instead of
surfacing only a client `TimeoutError`.

## Tuning a run

Set as `workflow_dispatch` inputs, or exported before `run.sh`. Blank falls back to the backend's default.

* `SCALER_IT_NUM_TASKS` -- tasks per burst wave.
* `SCALER_IT_TASK_SECONDS` -- sleep per task. Timeouts stay fixed, so a much bigger value can exceed them.
* `SCALER_IT_WATERFALL_POLICY` -- a raw `waterfall_v1` policy for the waterfall topologies, passed to the
  scheduler verbatim. One rule per line, `priority,worker_manager_id[,max_task_concurrency]`. Ids are
  `wm-<backend>-p<position>`, reported on the `[e2e]` line at start-up; an unknown id fails fast. Read back
  with the scheduler's own parser, so a cap resizes that manager's pool too and the spill threshold cannot
  drift from the pool it provisions.

```bash
# a bigger, slower cross-backend waterfall: ecs (priority 1, cap 4) spilling to ec2 (priority 2)
SCALER_IT_NUM_TASKS=60 SCALER_IT_TASK_SECONDS=0.3 \
  SCALER_IT_WATERFALL_POLICY=$'1,wm-ecs-p1,4\n2,wm-ec2-p2' ./scripts/e2e/run.sh ecs_ec2
```

Also: `SCALER_IT_CONTAINER_CLI` (default `docker`), `SCALER_IT_REBUILD=1` (force an image rebuild),
`SCALER_IT_MANYLINUX_WHEEL_DIR`, `SCALER_IT_FLOCI_IMAGE`.

## Adding a scenario, backend, or topology

The three axes are independent:

* **A scenario** -- add a `(test_case, deployment)` function to `scenarios.py`: read timing off
  `deployment.profile`, submit via `deployment.tasks`, observe with `deployment.running()` or per-manager
  `deployment.handles`. List it on the topologies that should run it in `test_scaling.py`.
* **A backend** -- add a class to `backends.py` satisfying `WorkerManagerBackend`: a `Profile` matched to
  its boot latency, `ensure_image()`, and `provision()` returning a `ManagerHandle` with a prefix-scoped
  `ContainerPool`. If it needs new shared infra (an emulator, a wheel server), extend `deploy()` behind a
  `needs_*` flag.
* **A topology** -- add a class to `test_scaling.py`: an ordered list of `ManagerSpec`s (priority =
  position; cap the higher tiers to force spill) and the scenarios it runs. A cross-backend waterfall is
  just `[ManagerSpec(BackendA(), cap=N), ManagerSpec(BackendB())]`. Add its name to the workflow's
  `topology` choices.

## Notes

* **Images.** `worker.Dockerfile` bakes in the wheel from `dist/` plus the custom capnp/kj runtime libs, so
  a container runs the same build and wire protocol as the host scheduler with no bind-mounts.
  `ecs.Dockerfile` adds an entrypoint that execs the `COMMAND` env var, because that is how the shipped ECS
  provisioner delivers the worker command.
* **The EC2 wheel.** The plain `dist/` wheel is `linux_x86_64` and will not load on Amazon Linux 2023's
  older glibc, so the EC2 topologies need a `manylinux` wheel of the current source
  (`scripts/e2e/build_cibuildwheel.sh`), served to the instance over the bridge gateway. Nothing is
  compiled inside the instance.
* **Two floci shims** (`ec2.Dockerfile`, `_install_floci_ec2_compat`) exist only because floci's AL2023
  image lacks `tar`, and its `DescribeLaunchTemplates` returns empty where real AWS raises. Both are
  harness-side, so the shipped provisioner stays unmodified.
* **The container backend keeps `--per-worker-task-queue-size 1`** deliberately, so the test double stays a
  stable scale-up harness. Dropping it breaks the scale-up scenarios.
