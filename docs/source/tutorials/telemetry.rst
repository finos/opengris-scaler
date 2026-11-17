Telemetry
=========

Scaler exports metrics and logs to an `OpenTelemetry <https://opentelemetry.io/>`_ collector over OTLP/HTTP.

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

Services
--------

Each process reports itself with ``service.namespace=scaler`` and one of these ``service.name`` values:

- ``scaler-scheduler``
- ``scaler-worker-manager``
- ``scaler-worker``: the worker agent
- ``scaler-processor``: the process that runs task functions

Resource attributes ``scaler.worker.id``, ``scaler.worker_manager.id``, and ``scaler.unit.id`` identify workers and worker managers.

Metrics
-------

The scheduler exports these metrics every ``status_report_interval_seconds``, except the event loop lag, which it samples every second:

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
