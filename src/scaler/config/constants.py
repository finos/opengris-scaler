# how long SchedulerProcess.wait_until_ready waits for the scheduler to listen
SCHEDULER_READINESS_TIMEOUT_SECONDS = 30

# vanilla scaling adds a worker above VANILLA_UPPER_TASK_RATIO tasks per worker, shrinks below VANILLA_LOWER_TASK_RATIO
VANILLA_LOWER_TASK_RATIO = 1
VANILLA_UPPER_TASK_RATIO = 10

# a unit that stays active this long proves the unit recipe works, so the crash-loop backoff starts over
UNIT_STABLE_SECONDS = 60

# the crash-loop backoff doubles on each consecutive loss, up to this many times
MAX_RESTART_BACKOFF_DOUBLINGS = 5
