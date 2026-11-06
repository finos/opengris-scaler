# how long a parent waits for a scheduler or object storage process it started to listen
READINESS_TIMEOUT_SECONDS = 30

# the monitor binds next to the scheduler unless configured otherwise
DEFAULT_MONITOR_PORT_OFFSET = 2

# vanilla scaling adds a worker above VANILLA_UPPER_TASK_RATIO tasks per worker, shrinks below VANILLA_LOWER_TASK_RATIO
VANILLA_LOWER_TASK_RATIO = 1
VANILLA_UPPER_TASK_RATIO = 10
