Telemetry
=========

Scaler exports traces, metrics, and logs to an `OpenTelemetry <https://opentelemetry.io/>`_ collector over OTLP/HTTP.

Enable export
-------------

1. Install the SDK and exporter on every machine that runs a scheduler, worker manager, or worker:

   .. code-block:: bash

       pip install opengris-scaler[otel]

2. Set the collector endpoint in the environment of each Scaler process:

   .. code-block:: bash

       export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318

- A process exports only when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set, and ``OTEL_SDK_DISABLED=true`` turns export off.
- Workers and processors inherit the environment of the worker manager that starts them.
- The SDK reads every other setting from the standard `OTEL_* variables <https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/>`_, for example ``OTEL_EXPORTER_OTLP_HEADERS`` and ``OTEL_METRIC_EXPORT_INTERVAL``.

Relay through the scheduler
---------------------------

Workers that cannot reach a collector, such as workers in cloud units, send their telemetry through the scheduler:

1. Set ``OTEL_EXPORTER_OTLP_ENDPOINT`` for the scheduler.
2. Start the worker manager with ``--telemetry-relay``, or set ``telemetry_relay = true`` in its ``[[worker_manager]]`` section.

- Its workers and processors then export into a buffer, and send it to the scheduler every second; a processor also sends after each task.
- The scheduler forwards each batch unparsed to ``<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/traces``, ``/v1/metrics``, or ``/v1/logs`` with ``OTEL_EXPORTER_OTLP_HEADERS``, from a thread.
- A relaying worker ignores ``OTEL_EXPORTER_OTLP_ENDPOINT``, and needs the ``otel`` extra installed.
- A full buffer, on a worker or in the scheduler, drops new batches and logs a warning with the count.
- A scheduler without ``OTEL_EXPORTER_OTLP_ENDPOINT`` drops relayed telemetry, and logs a warning once.

Services
--------

Each process reports itself with ``service.namespace=scaler`` and one of these ``service.name`` values:

- ``scaler-scheduler``
- ``scaler-worker-manager``
- ``scaler-worker``: the worker agent
- ``scaler-processor``: the process that runs task functions

Resource attributes ``scaler.worker.id``, ``scaler.worker_manager.id``, and ``scaler.unit.id`` identify workers and worker managers.

Traces
------

Each task is one trace:

.. code-block:: text

    scaler.client.submit                 the client hands the task to the scheduler
    └─ scaler.scheduler.task             from arrival to a terminal state, one span event per transition
       ├─ scaler.scheduler.queued        waiting for a worker with room in its queue
       └─ scaler.worker.task             from arrival at the worker to the result or cancel it sends back
          ├─ scaler.worker.queued        waiting in the worker's queue for its processor
          └─ scaler.processor.execute    the function call, recording the exception of a failed task
             ├─ scaler.processor.fetch_objects
             ├─ <spans the task function starts>
             └─ scaler.processor.store_result

- A task requeued after its worker died or a balance cancel gets another ``scaler.scheduler.queued`` span, and a suspended task another ``scaler.worker.queued`` span.
- The client does not install the SDK: ``scaler.client.submit`` records only when the calling application configured OpenTelemetry, and joins the caller's trace.
- Without a client span, ``scaler.scheduler.task`` starts the trace.
- The task function runs inside ``scaler.processor.execute``, so spans it starts with the OpenTelemetry API nest under it.
- ``scaler.scheduler.task`` carries ``scaler.task.id``, ``scaler.task.function``, and the terminal ``scaler.task.state``, and ends with an error status when the task failed.
- With a client span, every Scaler process follows the client's sampling decision.
- Without one, the scheduler decides by the standard ``OTEL_TRACES_SAMPLER`` variable, and the worker and processor follow it.

Each change of a worker manager's desired concurrency is one trace:

.. code-block:: text

    scaler.scheduler.scaling_decision    the scaling policy changed a manager's desired task concurrency
    └─ scaler.worker_manager.reconcile   the manager starts or stops units, including its cloud API calls
       └─ scaler.worker.start            a native worker, from process start to its first heartbeat echo

- ``scaler.scheduler.scaling_decision`` carries ``scaler.worker_manager.id``, ``scaler.worker_manager.desired_task_concurrency``, and ``scaler.worker_manager.previous_task_concurrency``.
- ``scaler.worker_manager.reconcile`` carries ``scaler.worker_manager.desired_units`` and ``scaler.worker_manager.current_units``, and ends with an error status when starting or stopping units raised.
- ``scaler.worker.start`` ends with an error status when the worker exits before it reaches the scheduler.
- The trace ends at the worker manager for workers that a cloud unit starts.

Baggage
-------

- `Baggage <https://opentelemetry.io/docs/concepts/signals/baggage/>`_ the client sets travels with each task, and every span the scheduler, worker, and processor start copies it into its attributes.
- Tag a job once on the client, and every span of its tasks carries the tag:

  .. code-block:: python

      from opentelemetry import baggage, context

      token = context.attach(baggage.set_baggage("job", "backtest-42"))
      try:
          futures = [client.submit(simulate, seed) for seed in range(100)]
      finally:
          context.detach(token)

Metrics
-------

The scheduler refreshes these values every ``status_report_interval_seconds``, samples the event loop lag every second, and exports every ``OTEL_METRIC_EXPORT_INTERVAL``:

- ``scaler.scheduler.tasks``: tasks per ``state``; terminal states count every task that reached them
- ``scaler.scheduler.workers``: connected workers
- ``scaler.scheduler.processors``: processors per ``state`` (``busy``, ``idle``, ``suspended``)
- ``scaler.scheduler.clients``: connected clients
- ``scaler.scheduler.objects``: objects the scheduler tracks
- ``scaler.object_storage.bytes``: bytes the object storage server holds
- ``scaler.worker_manager.pending_workers``: workers requested from each worker manager that have not connected yet
- ``scaler.scheduler.event_loop.lag``: how late the scheduler's event loop wakes a sleeping coroutine, in seconds

Logs
----

- Every process exports the records of the ``scaler`` logger at its configured logging level.
- The scheduler's lifecycle records carry an ``event.name`` attribute and the IDs involved:

  - ``scaler.worker.connected``, ``scaler.worker.draining``, ``scaler.worker.disconnected`` (with ``scaler.worker.disconnect_reason``)
  - ``scaler.client.connected``, ``scaler.client.disconnected``
