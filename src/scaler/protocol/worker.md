# Custom Worker Implementation

## Overview

The existed python worker implementation can work with scheduler and scaler protocol is under `scaler.worker`

- workers are connecting to 1 single TCP port of scheduler by using zmq
- each worker has a fixed length task queue to store the incoming tasks
- in Task message or `TaskResult` message, it will contain object ids instead of actual data, once worker received one
  task, it should ask scheduler for the actual data by sending `ObjectRequest` message
- once worker finished the task, it should send `TaskResult` message back to scheduler, the `TaskResult` message should
  have the object id of the result, and worker should send `ObjectInstruction` message to scheduler to store the result
- each worker have a unique id and can be identified by the scheduler that can be set by zmq (see below setup)
- worker is responsible for sending the heartbeat to scheduler, if scheduler didn't receive heartbeat from worker for
  a period of time, then scheduler will consider the worker is dead and will reallocate all tasks to other workers, also
  heartbeat message will contain the worker's resource usage information and queue capacity and usage status
- each Task is tied to a source, each source has a dedicated serializer, the source is used to choose the proper
  serializer for deserializing the function and arguments, worker will need retrieve the serializer bytes using an
  `ObjectRequest` message, and then deserialize the serializer bytes using cloudpickle, deserialized serializer is
  implements the interface `scaler.client.serializer.mixins.Serializer`
- When worker received TaskCancel message, it should cancel the task regardless of tasks status with the given task ID
  and send a `TaskResult` back to the scheduler

- worker can request to scheduler to balance the tasks, scheduler will send the tasks to be given up by the worker (this
will be replaced by TaskCancel message in the future)

## Setup

The zmq `IDENTITY` must be unique. The zmq `SNDHWM` and `RCVHWM` should both be set to 0 to prevent messages from being
dropped unexpectedly, and it should be DEALER socket type.

All message will be list of bytes represented by frames, the first frame is the message type, and the rest of the frames
are the message data. One message is the whole list of frames.

To send a single message using pyzmq, use the `send_multipart` method:

```python
# Synchronous API
socket.send_multipart(frames)

# Asyncronous API
await socket.send_multipart(frames)
```

Each message may contain multiple zmq frames. To read a single message using pyzmq, use the `recv_multipart` method:

```python
# Synchronous API
frames = socket.recv_multipart()

# Asyncronous API
frames = await socket.recv_multipart()
```

All below messages, please see `scaler.protocol.python.message.py` for actual message structure to help you understand

## Recv messages

### Task `TK`

| message_type | task_id | source  | metadata | func_object_id | arg 1 type | arg 1 data | (...) | arg N type | arg N data |
|:------------:|:-------:|:-------:|:--------:|:--------------:|:----------:|:----------:|:-----:|:----------:|:----------:|
|    b"TK"     | X bytes | X bytes | X bytes  |    X bytes     |    b"R"    |  X bytes   |       |    b"R"    |  X bytes   |

* task_id: Task ID
* source: Source ID. This is used to choose the proper serializer for deserializing the
  function and arguments. The serializer object ID is md5 hash of `source + b"serializer"` and the serializer object
  bytes data must first be deserialized using cloudpickle. source is essentially the client id, indicate which client
  this task it belongs to, please refer to `scaler.utility.object_utility` for `generate_serializer_object_id`
* metadata: The metadata of the task, can be empty bytes like `b""`
* func_object_id: Function object ID
* arg type: Must be type `b"R"` for ObjectID
* arg data: Object ID

After executing `Task`, the worker should send a `TaskResult` message with the final task result.

### TaskCancel `TC`

| message_type | task_id |
|:------------:|:-------:|
|    b"TC"     | X bytes |

* task_id: Task ID to cancel

When a `TaskCancel` message is received, the worker should cancel the task with the given task ID and send a
`TaskResult`

### ObjectInstruction `OI`

| message_type | source  | type | num_object_ids | num_object_names | num_object_bytes | object 1 id | (...) | object N id |
|:------------:|:-------:|:----:|:--------------:|:----------------:|:----------------:|:-----------:|:-----:|:-----------:|
|    b"OI"     | X bytes | b"D" |  unsigned int  |   unsigned int   |   unsigned int   |   X bytes   |       |   X bytes   |

* source: Source ID
* type: Must be `b"D` for Delete
* num_object_ids: Number of object IDs
* num_object_names: Number of object names, value must be zero
* num_object_bytes: Number of object bytes, value must be zero
* object id: Object ID

When a Delete `ObjectInstruction` message is received, the worker should delete the objects with the given object IDs on
worker side

### ObjectResponse `OA`

| message_type |  type  | num_object_ids | num_object_names | num_object_bytes | object 1 id | (...) | object N id | object 1 name | (...) | object N name | object 1 bytes | (...) | object N bytes |
|:------------:|:------:|:--------------:|:----------------:|:----------------:|:-----------:|:-----:|:-----------:|:-------------:|:-----:|:-------------:|:--------------:|:-----:|:--------------:|
|    b"OA"     | 1 byte |  unsigned int  |   unsigned int   |   unsigned int   |   X bytes   |       |   X bytes   |    X bytes    |       |    X bytes    |    X bytes     |       |    X bytes     |

* type: `b"C"` for object found, `b"N"` for object not found
* num_object_ids: Number of object IDs
* num_object_names: Number of object names
* num_object_bytes: Number of object bytes
* object id: Object ID
* object name: Object name
* object bytes: Object bytes

### BalanceRequest `BQ` (will be replaced by TaskCancel in the future, low priority to implement this)

| message_type |  num_tasks   |
|:------------:|:------------:|
|    b"BQ"     | unsigned int |

* num_tasks: Number of tasks to give up

When a `BalanceRequest` message is received, the worker should send a `BalanceResponse` message with num_tasks number of
task IDs.

### WorkerHeartbeatEcho `HE` (optional)

| message_type |  empty  |
|:------------:|:-------:|
|    b"HE"     | 0 bytes |

A `WorkerHeartbeatEcho` message indicates that the scheduler has received the worker's `WorkerHeartbeat` message.

### ClientDisconnect `CS` (optional)

| message_type | type |
|:------------:|:----:|
|    b"CS"     | b"S" |

* type: Must be type `b"S"` for Shutdown.

When a Shutdown `ClientDisconnect` message is received, the worker should shutdown.

## Send messages

### TaskResult `TR`

| message_type | task_id | status | result  | metadata |
|:------------:|:-------:|:------:|:-------:|:--------:|
|    b"TR"     | X bytes | 1 byte | X bytes | X bytes  |

* task_id: Task ID
* status: `b"S"` for Success, `b"F"` for Failed, `b"C"` for Canceled, `b"K"` for WorkerDied, `b"W"` for NoWorker, `b"I"`
  for Inactive, `b"R"` for Running, and `b"X"` for Canceling
* result: Task result object ID
* metadata: Task metadata

Worker must submit a Create `ObjectInstruction` message BEFORE returning the task result containing a task result object
ID.

### ObjectInstruction `OI`

| message_type | source  | type | num_object_ids | num_object_names | num_object_bytes | object 1 id | (...) | object N id | object 1 name | (...) | object N name | object 1 bytes | (...) | object N bytes |
|:------------:|:-------:|:----:|:--------------:|:----------------:|:----------------:|:-----------:|:-----:|:-----------:|:-------------:|:-----:|:-------------:|:--------------:|:-----:|:--------------:|
|    b"OI"     | X bytes | b"C" |  unsigned int  |   unsigned int   |   unsigned int   |   X bytes   |       |   X bytes   |    X bytes    |       |    X bytes    |    X bytes     |       |    X bytes     |

* source: Source ID (this should be the same as the source in the corresponding `Task` message)
* type: Must be `b"C` for Create
* num_object_ids: Number of object IDs
* num_object_names: Number of object names
* num_object_bytes: Number of object bytes
* object id: Object ID (please use uuid.uuid4.bytes for object id)
* object name: Object name
* object bytes: Object bytes

### ObjectRequest `OR`

| message_type | type | object 1 id | (...) | object N id |
|:------------:|:----:|:-----------:|:-----:|:-----------:|
|    b"OR"     | b"A" |   X bytes   |       |   X bytes   |

* type: Must be `b"A"` for Get
* object id: Object ID

When received a `Task` message, all the functions and arguments are just object IDs, the worker will need get object
content by sending `ObjectRequest` message to the scheduler, the scheduler will reply with `ObjectResponse` message,
then worker can deserialize the object content and start executing the task

### BalanceResponse `BR`

| message_type | task 1 id | (...) | task N id |
|:------------:|:---------:|:-----:|:---------:|
|    b"BR"     |  X bytes  |       |  X bytes  |

* task id: Task ID

### WorkerHeartbeat `HB`

| message_type |   agent_cpu    |     agent_rss      |   worker_cpu   |     worker_rss     |      rss_free      |  queued_tasks  |  latency_us  | initialized | has_task | task_lock |
|:------------:|:--------------:|:------------------:|:--------------:|:------------------:|:------------------:|:--------------:|:------------:|:-----------:|:--------:|:---------:|
|    b"HB"     | unsigned short | unsigned long long | unsigned short | unsigned long long | unsigned long long | unsigned short | unsigned int |    _Bool    |  _Bool   |   _Bool   |

* agent_cpu: Agent CPU usage
* agent_rss: Agent resident set size in bytes
* worker_cpu: Worker CPU usage
* worker_rss: Worker resident set size in bytes
* rss_free: Free memory in bytes
* queued_tasks: Number of queued tasks
* latency_us: Latency in microseconds
* initialized: Worker initialized
* has_task: Worker has task
* task_lock: Worker task lock

Worker must send a `WorkerHeartbeat` message every heartbeat interval (at least 1 second) or else the scheduler will
consider the worker dead.

### DisconnectRequest `DR` (optional)

| message_type | worker  |
|:------------:|:-------:|
|    b"DR"     | X bytes |

* worker: Worker ID

When a `DisconnectRequest` message is sent, the worker should disconnect from the scheduler.
