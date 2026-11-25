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
- A cloud worker manager (ECS, ORB AWS EC2, OCI) passes every ``OTEL_*`` variable it has to the units it starts, so a unit's manager, workers, and processors export the same way.
- ``otlp_endpoint`` in the ``[scheduler]`` or a ``[[worker_manager]]`` section (``--otlp-endpoint``) sets the endpoint for that process in place of ``OTEL_EXPORTER_OTLP_ENDPOINT``, so one configuration file can send each component to the collector address it reaches.
- Workers that reach the collector at another address, such as cloud units behind NAT, export to ``worker_otlp_endpoint`` (``--worker-otlp-endpoint``) of their worker manager instead; it applies to local workers and to units alike.
- Both replace ``OTEL_EXPORTER_OTLP_ENDPOINT`` only, so a signal-specific endpoint such as ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT``, headers, and certificates still come from the environment.
- The cloud provider stores those variables with the unit (the ECS task overrides, the EC2 user data, the OCI container): put a secret such as an ``OTEL_EXPORTER_OTLP_HEADERS`` token there only if those places may hold it.
- A cloud worker manager whose units install from ``requirements_txt`` refuses to start when its units would export (an endpoint, ``--worker-otlp-endpoint``, or ``--telemetry-relay``) but the requirements install ``opengris-scaler`` without the ``otel`` or ``all`` extra.
- On Windows, a worker or processor that is stopped loses the telemetry it has not exported yet, up to ``OTEL_BSP_SCHEDULE_DELAY`` of spans: Windows stops them without letting them flush.
- The SDK reads every other setting from the standard `OTEL_* variables <https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/>`_, for example ``OTEL_EXPORTER_OTLP_HEADERS`` and ``OTEL_METRIC_EXPORT_INTERVAL``.

Relay through the scheduler
---------------------------

Workers that cannot reach a collector, such as workers in cloud units, send their telemetry through the scheduler:

1. Set ``OTEL_EXPORTER_OTLP_ENDPOINT`` for the scheduler.
2. Start the worker manager with ``--telemetry-relay``, or set ``telemetry_relay = true`` in its ``[[worker_manager]]`` section.

- Its workers and processors then export into a buffer, and send it to the scheduler every second; a processor also sends after each task.
- The scheduler forwards each batch unparsed to ``<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/traces``, ``/v1/metrics``, or ``/v1/logs`` with ``OTEL_EXPORTER_OTLP_HEADERS``, from a thread.
- A relaying worker ignores ``OTEL_EXPORTER_OTLP_ENDPOINT``, and needs the ``otel`` extra installed; ``telemetry_relay`` and ``worker_otlp_endpoint`` are mutually exclusive.
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
- Every span of a task carries ``scaler.client.id``, the client that submitted it, through baggage; a client created inside a task replaces its parent's, and its tasks' spans nest under that task's ``scaler.processor.execute`` in the same trace.
- With a client span, every Scaler process follows the client's sampling decision.
- Without one, the scheduler decides by the standard ``OTEL_TRACES_SAMPLER`` variable, and the worker and processor follow it.

Each change of a worker manager's desired concurrency is one trace, with a span for each unit it creates:

.. code-block:: text

    scaler.scheduler.scaling_decision        the scaling policy changed a manager's desired task concurrency
    └─ scaler.worker_manager.create_unit     the manager creates one unit, including its cloud API calls
       ├─ scaler.worker.start                a native worker, from process start to its first heartbeat echo
       └─ scaler.worker_manager.create_unit  the manager of a cloud unit, once the unit booted, starting a worker
          └─ scaler.worker.start

- ``scaler.scheduler.scaling_decision`` carries ``scaler.worker_manager.id``, ``scaler.worker_manager.desired_task_concurrency``, and ``scaler.worker_manager.previous_task_concurrency``.
- ``scaler.worker_manager.create_unit`` carries ``scaler.unit.id``, and ends with an error status when creating the unit raised.
- ``scaler.worker.start`` ends with an error status when the worker exits before it reaches the scheduler.
- A cloud worker manager starts each unit with ``TRACEPARENT`` and ``BAGGAGE`` set to the ``scaler.worker_manager.create_unit`` span that started it, and the unit's manager creates its workers under them.
- The gap between the end of a cloud unit's ``create_unit`` span and the start of its manager's ``create_unit`` spans is the unit's boot time.
- Each call that destroys a unit is a ``scaler.worker_manager.destroy_unit`` span, carrying ``scaler.unit.id`` and ending with an error status when the call raised; a retried destroy gets another.
- A unit shed to scale down is destroyed under the ``scaler.scheduler.scaling_decision`` that lowered the desired concurrency, after ``--scale-down-cooldown-seconds``.
- A unit destroyed because it was lost, because it exited, or because its manager is stopping starts a trace of its own.
- A worker manager spans each AWS API call boto3 makes (ECS, ORB AWS EC2), such as ``EC2.DescribeInstances``, under the ``create_unit`` or ``destroy_unit`` span that made it, with ``rpc.service``, ``rpc.method``, ``aws.region``, and ``aws.request_id``.
- Those calls carry the span's context to AWS in the ``X-Amzn-Trace-Id`` header.
- OCI API calls have no spans: the OCI SDK has no OpenTelemetry support.

Tags
----

Tag a client's tasks, and every span of those tasks carries the tags as attributes:

.. code-block:: python

    with Client(address=address, tags={"team": "rates", "job": "backtest-42"}) as client:
        results = client.map(simulate, range(1000))

        with client.tags(stage="aggregate"):
            client.submit(aggregate, results)

- ``tags`` on the client tags everything it submits; a ``client.tags()`` block adds tags to the submits inside it.
- A task's tags are, lowest first: the `baggage <https://opentelemetry.io/docs/concepts/signals/baggage/>`_ current when it is submitted, the client's tags, its ``tags()`` blocks with the innermost winning, and ``scaler.client.id``.
- A ``tags()`` block applies to that client only, in the thread or asyncio task that entered it.
- Values are sent as strings, and keys starting with ``scaler.`` are reserved.
- A client created inside a task inherits the task's tags, and adds its own over them.
- Tags travel as W3C baggage: user code that calls other services through instrumented HTTP libraries sends them on, so a tag must never hold a secret.

Spans in task code
------------------

A task function creates spans with the OpenTelemetry API; they nest under ``scaler.processor.execute`` and carry the task's tags:

.. code-block:: python

    from opentelemetry import trace

    tracer = trace.get_tracer("my_package")

    def simulate(seed: int) -> float:
        with tracer.start_as_current_span("simulate.load") as span:
            span.set_attribute("seed", seed)
            data = load(seed)
        with tracer.start_as_current_span("simulate.run"):
            return run(data)

- The processor exports them with its own spans: the task code needs only ``opentelemetry-api``, which Scaler depends on.
- Without ``OTEL_EXPORTER_OTLP_ENDPOINT`` set for the worker, the calls are no-ops.

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
- ``scaler.scheduler.event_loop.lag``: how late the scheduler's event loop wakes a sleeping coroutine, in seconds, as a histogram with bucket boundaries from 1 ms to 10 s

Logs
----

- Every process exports the records of the ``scaler`` logger at its configured logging level.
- The scheduler's lifecycle records carry an ``event.name`` attribute and the IDs involved:

  - ``scaler.worker.connected``, ``scaler.worker.draining``, ``scaler.worker.disconnected`` (with ``scaler.worker.disconnect_reason``)
  - ``scaler.client.connected``, ``scaler.client.disconnected``
