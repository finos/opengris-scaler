"""Names of the spans, span events, log events, metrics, and attributes Scaler exports over OpenTelemetry."""

# resource and span attributes
SERVICE_NAMESPACE = "scaler"
CLIENT_ID_ATTRIBUTE = "scaler.client.id"
WORKER_ID_ATTRIBUTE = "scaler.worker.id"
WORKER_MANAGER_ID_ATTRIBUTE = "scaler.worker_manager.id"
UNIT_ID_ATTRIBUTE = "scaler.unit.id"
TASK_ID_ATTRIBUTE = "scaler.task.id"
FUNCTION_NAME_ATTRIBUTE = "scaler.task.function"
TASK_STATE_ATTRIBUTE = "scaler.task.state"
WORKER_DISCONNECT_REASON_ATTRIBUTE = "scaler.worker.disconnect_reason"
STATE_ATTRIBUTE = "state"
DESIRED_TASK_CONCURRENCY_ATTRIBUTE = "scaler.worker_manager.desired_task_concurrency"
PREVIOUS_TASK_CONCURRENCY_ATTRIBUTE = "scaler.worker_manager.previous_task_concurrency"

# spans
CLIENT_SUBMIT_SPAN = "scaler.client.submit"
SCHEDULER_TASK_SPAN = "scaler.scheduler.task"
SCHEDULER_QUEUED_SPAN = "scaler.scheduler.queued"
WORKER_TASK_SPAN = "scaler.worker.task"
WORKER_QUEUED_SPAN = "scaler.worker.queued"
PROCESSOR_EXECUTE_SPAN = "scaler.processor.execute"
PROCESSOR_FETCH_OBJECTS_SPAN = "scaler.processor.fetch_objects"
PROCESSOR_STORE_RESULT_SPAN = "scaler.processor.store_result"
SCHEDULER_SCALING_DECISION_SPAN = "scaler.scheduler.scaling_decision"
WORKER_MANAGER_CREATE_UNIT_SPAN = "scaler.worker_manager.create_unit"
WORKER_START_SPAN = "scaler.worker.start"

# events on the worker task span; the scheduler task span names its events after the task event classes
WORKER_TASK_SUSPENDED_EVENT = "suspended"
WORKER_TASK_CANCELED_EVENT = "canceled"

# log records carry the event name in this attribute
EVENT_NAME_ATTRIBUTE = "event.name"
WORKER_CONNECTED_EVENT = "scaler.worker.connected"
WORKER_DRAINING_EVENT = "scaler.worker.draining"
WORKER_DISCONNECTED_EVENT = "scaler.worker.disconnected"
CLIENT_CONNECTED_EVENT = "scaler.client.connected"
CLIENT_DISCONNECTED_EVENT = "scaler.client.disconnected"

# metrics
SCHEDULER_METER = "scaler.scheduler"
SCHEDULER_TASKS_METRIC = "scaler.scheduler.tasks"
SCHEDULER_WORKERS_METRIC = "scaler.scheduler.workers"
SCHEDULER_PROCESSORS_METRIC = "scaler.scheduler.processors"
SCHEDULER_CLIENTS_METRIC = "scaler.scheduler.clients"
SCHEDULER_OBJECTS_METRIC = "scaler.scheduler.objects"
SCHEDULER_EVENT_LOOP_LAG_METRIC = "scaler.scheduler.event_loop.lag"
OBJECT_STORAGE_BYTES_METRIC = "scaler.object_storage.bytes"
WORKER_MANAGER_PENDING_WORKERS_METRIC = "scaler.worker_manager.pending_workers"
