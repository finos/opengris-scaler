import os

# ==============
# SYSTEM OPTIONS

# object clean up time interval
CLEANUP_INTERVAL_SECONDS = 1

# status report interval, used by poke or scaled monitor
STATUS_REPORT_INTERVAL_SECONDS = 1

# number of seconds for profiling
PROFILING_INTERVAL_SECONDS = 1

# cap'n proto only allow Data/Text/Blob size to be as big as 500MB
CAPNP_DATA_SIZE_LIMIT = 2**29 - 1

# message size limitation, max can be 2**64
CAPNP_MESSAGE_SIZE_LIMIT = 2**64 - 1

# ==========================
# SCHEDULER SPECIFIC OPTIONS

# number of threads for zmq socket to handle
DEFAULT_IO_THREADS = 1

# if all workers are full and busy working, this option determine how many additional tasks scheduler can receive and
# queued, if additional number of tasks received exceeded this number, scheduler will reject tasks
DEFAULT_MAX_NUMBER_OF_TASKS_WAITING = -1

# if didn't receive heartbeat for following seconds, then scheduler will treat worker as dead and reschedule unfinished
# tasks for this worker
DEFAULT_WORKER_TIMEOUT_SECONDS = 60

# if didn't receive heartbeat for following seconds, then scheduler will treat client as dead and cancel remaining
# tasks for this client
DEFAULT_CLIENT_TIMEOUT_SECONDS = 600

# number of seconds for load balance, if value is 0 means disable load balance
DEFAULT_LOAD_BALANCE_SECONDS = 1

# when load balance advice happened repeatedly and always be the same, we issue load balance request when exact repeated
# times happened
DEFAULT_LOAD_BALANCE_TRIGGER_TIMES = 2

# number of tasks can be queued to each worker on scheduler side
DEFAULT_PER_WORKER_QUEUE_SIZE = 1000

# =======================
# WORKER SPECIFIC OPTIONS

# number of workers, echo worker use 1 process
DEFAULT_NUMBER_OF_WORKER = os.cpu_count() - 1

# number of seconds that worker agent send heartbeat to scheduler
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 2

# number of seconds the object cache kept in worker's memory
DEFAULT_OBJECT_RETENTION_SECONDS = 60

# number of seconds worker doing garbage collection
DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS = 30

# number of bytes threshold for worker process that trigger deep garbage collection
DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES = 1024 * 1024 * 1024

# default task timeout seconds, 0 means never timeout
DEFAULT_TASK_TIMEOUT_SECONDS = 0

# number of seconds that worker agent wait for processor to finish before killing it
DEFAULT_PROCESSOR_KILL_DELAY_SECONDS = 3

# number of seconds without scheduler contact before worker shuts down
DEFAULT_WORKER_DEATH_TIMEOUT = 5 * 60

# the global client name for get log, right now, all client shared remote log, task need have client information to
# deliver log
DUMMY_CLIENT = b"dummy_client"

# if true, suspended worker's processors will be actively suspended with a SIGTSTP signal, otherwise a synchronization
# event will be used.
DEFAULT_HARD_PROCESSOR_SUSPEND = False
