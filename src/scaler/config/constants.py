# how long a parent waits for a scheduler or object storage process it started to listen
READINESS_TIMEOUT_SECONDS = 30

# the monitor binds next to the scheduler unless configured otherwise
DEFAULT_MONITOR_PORT_OFFSET = 2

# vanilla scaling adds a worker above VANILLA_UPPER_TASK_RATIO tasks per worker, shrinks below VANILLA_LOWER_TASK_RATIO
VANILLA_LOWER_TASK_RATIO = 1
VANILLA_UPPER_TASK_RATIO = 10

# how often a worker manager's unit controller reconciles; it reads only state that messages wrote
WORKER_MANAGER_UNIT_CONTROLLER_INTERVAL_SECONDS = 1

# seconds a worker manager waits before it creates a unit after losing one; doubles on each consecutive loss
WORKER_MANAGER_RESTART_BACKOFF_SECONDS = 1

# a unit that stays active this long proves the unit recipe works, so the crash-loop backoff starts over
WORKER_MANAGER_UNIT_STABLE_SECONDS = 60

# the crash-loop backoff doubles on each consecutive loss, up to this many times
WORKER_MANAGER_MAX_RESTART_BACKOFF_DOUBLINGS = 5
