# Roles

The communication protocol include 3 roles: client, scheduler and worker:

- client is upstream of scheduler, scheduler is upstream of worker
- worker is downstream of scheduler, scheduler is downstream of client

```plaintext
                                                     +--------------+
                      +-----------------+    TCP     |              |
                      |                 +------------+    worker    |
+-----------+         |                 |            |              |
|           |   TCP   |                 |            +--------------+
|  client   +---------+                 |
|           |         |                 |            +--------------+
+-----------+         |                 |    TCP     |              |
                      |    scheduler    +------------+    worker    |
+-----------+         |  (object store) |            |              |
|           |   TCP   |                 |            +--------------+
|  client   +---------+                 |
|           |         |                 |            +--------------+
+-----------+         |                 |    TCP     |              |
                      |                 +------------+    worker    |
                      +-----------------+            |              |
                                                     +--------------+

```

each client to scheduler and each worker to scheduler only maintains 1 TCP connection

# Message format

Scaler is using capnp library to serialize/deserialize and use zmq to communicate between client and scheduler and
worker

# Message Type Category

In general, there are 2 categories of the message types: object and task

object normally has an object id associated with actual object data, object data is immutable bytes, serialized by
client/worker, and deserialized by client/worker. protocol didn't define the way to serialize it, it's up to the
client/worker to decide

task is a function call, it has a task id associate with the actual function call, and the function call contains
function and series of arguments, but task message doesn't contain the actual function and arguments, instead it
contains object ids, workers are responsible to fetch the function/argument data from scheduler and deserialize and
execute the function call.

## Object Channel

Scheduler is the center of the object storage, client and worker are identical and can push

```plaintext
                                                      ObjectInstruction
                                                       ObjectResponse   +--------------+
                                                    +------------------>|              |
                                                    |                   |   Worker     |
+---------+                        +-----------+    |  +----------------+              |
|         |     ObjectRequest      |           +----+  | ObjectRequest  +--------------+
|         |   ObjectInstruction    |           |       |
|         +----------------------->|           |<------+
|  Client |                        | Scheduler |
|         |<-----------------------+           +-------+
|         |    ObjectResponse      |           |       | ObjectInstruction
|         |                        |           +<---+  | ObjectResponse +--------------+
+---------+                        +-----------+    |  +--------------->|              |
                                                    |                   |   Worker     |
                                                    +-------------------+              |
                                                         ObjectRequest  +--------------+

```

ObjectInstruction = b"OI"
client can send object instruction to scheduler, scheduler can send object instruction to worker
it has 2 subtypes: create b"C", delete b"D"
when subtype is create, it has to include:

- list of object id (type bytes)
- list of object names (type bytes)
- list of object bytes (type bytes)
  All above 3 lists, the number of items need match

ObjectRequest = b"OR"
ObjectResponse = b"OA"

## Task Channel

```plaintext
                                                             Task
                                                          TaskCancel    +--------------+
                                                    +-------------------+              |
                    Task                            |                   |   Worker     |
+---------+      TaskCancel        +-----------+    |  +----------------+              |
|         |      GraphTask         |           +----+  |  TaskResult    +--------------+
|         |    GraphTaskCancel     |           |       |
|         +------------------------+           |<------+
|  Client |                        | Scheduler |
|         |<-----------------------+           +-------+
|         |        TaskEcho        |           |       |     Task
|         |       TaskResult       |           +----+  |  TaskCancel    +--------------+
+---------+                        +-----------+    |  +--------------->|              |
                                                    |                   |   Worker     |
                                                    +-------------------+              |
                                                          TaskResult    +--------------+
```

Task = b"TK"
TaskEcho = b"TE"
TaskCancel = b"TC"
TaskResult = b"TR"
GraphTask = b"GT"
GraphTaskEcho = b"GE"
GraphTaskCancel = b"GC"
GraphTaskResult = b"GR"
