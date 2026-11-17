# how long SchedulerProcess.wait_until_ready waits for the scheduler to listen
SCHEDULER_READINESS_TIMEOUT_SECONDS = 30

# vanilla scaling adds a worker above VANILLA_UPPER_TASK_RATIO tasks per worker, shrinks below VANILLA_LOWER_TASK_RATIO
VANILLA_LOWER_TASK_RATIO = 1
VANILLA_UPPER_TASK_RATIO = 10

# OpenTelemetry SDK environment variables Scaler reads; the SDK reads every other OTEL_ variable itself
OTEL_ENVIRONMENT_PREFIX = "OTEL_"
OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
OTLP_HEADERS_ENV = "OTEL_EXPORTER_OTLP_HEADERS"
SDK_DISABLED_ENV = "OTEL_SDK_DISABLED"

# W3C trace context headers, and the environment variables that carry them into a process started inside a trace
TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"
BAGGAGE_HEADER = "baggage"
TRACEPARENT_ENV = "TRACEPARENT"
TRACESTATE_ENV = "TRACESTATE"
BAGGAGE_ENV = "BAGGAGE"

# how often the scheduler's lag monitor wakes; its oversleep is the time other callbacks held the event loop
EVENT_LOOP_LAG_SAMPLE_SECONDS = 1

# OTLP export requests a relaying process holds while its connector is slow; the SDK batches up to 512 spans per request
RELAY_BUFFER_CAPACITY = 1024

# how often a relaying worker sends its buffered batches; the SDK's own export delays are 5s for spans and logs
RELAY_SEND_INTERVAL_SECONDS = 1

# relayed export requests the scheduler holds for the collector, and how long it waits on one POST
FORWARD_QUEUE_CAPACITY = 1024
FORWARD_TIMEOUT_SECONDS = 10
