"""
Worker managers.

A worker manager keeps the scheduler supplied with workers. It heartbeats to the scheduler and reads
back the desired task concurrency. `runner.py` drives that loop, `unit_controller.py` owns the fleet
and converges it on the desired task concurrency, and `mixins.py` declares the `UnitProvisioner`
interface that all seven adapters implement: the mechanics of creating, polling, and destroying one unit.

The adapters are grouped by what a provisioner unit is:

- `native/` - one local worker process, spawned directly.
- `nested/` - one platform resource (an EC2 instance, an ECS task, an OCI Container Instance) that
  runs a native worker manager of its own, which in turn owns the worker processes.
- `proxy/` - one local proxy worker process that looks like an ordinary worker to the scheduler but
  submits each task to an external execution service and reports the result back.
"""
