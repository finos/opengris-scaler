# Design Document: WorkerAdapter Refactoring (Remove RESTful APIs)

## Overview

This document outlines the plan to refactor WorkerAdapter communication from RESTful APIs to Cap'n Proto messaging, aligning with the existing patterns used throughout the Scaler codebase.

## Definitions

- **WorkerAdapter**: Server component responsible for launching workers (e.g., `NativeWorkerAdapter`, `ECSWorkerAdapter`)
- **ScalingPolicy**: Component that decides when to scale (formerly `ScalingController`)
- **WorkerAdapterController**: New component that executes scaling commands and manages adapter connections

## Design Decisions

The following questions were raised during design review and resolved:

| Question | Decision |
|----------|----------|
| Should adapters connect TO scheduler, or scheduler connect TO adapters? | **Adapter connects TO scheduler.** Scheduler is the centralized component responsible for scheduling tasks. |
| Who owns `worker_groups` state? | **WorkerAdapterController owns `worker_groups` state.** ScalingPolicy receives state via snapshot (similar to how `InformationSnapshot` provides worker info). |
| Support multiple adapters per scheduler? | **Single adapter for now.** Interface designed to be adapter-agnostic for future multi-adapter support. |
| Are command responses sync or async? | **Async.** Everything is async in the scheduler event loop. WorkerAdapterController sends commands and fetches responses from the main event loop. |

## Constraints

1. `WorkerAdapterHeartbeat` & `WorkerAdapterHeartbeatEcho` will be used for keeping remote connections alive
2. `WorkerAdapterHeartbeat` carries adapter information (`max_worker_groups`, `workers_per_group`, `base_capabilities`) to scheduler
3. Separate message types for commands (`start_worker_group`, `shutdown_worker_group`) and responses
4. Policies should not communicate directly - they provide advice only

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SCHEDULER                                │
│  ┌──────────────────┐    ┌─────────────────────────────────┐    │
│  │  ScalingPolicy   │◄───│   WorkerAdapterController       │    │
│  │  (advice only)   │    │   - owns worker_groups state    │    │
│  │                  │    │   - sends commands              │    │
│  │  get_scaling_    │    │   - receives responses          │    │
│  │  advice()        │    │                                 │    │
│  │  → ScalingAdvice │    │                                 │    │
│  └──────────────────┘    └─────────────────────────────────┘    │
│                                      ▲                           │
│                                      │ Cap'n Proto               │
│                                      ▼                           │
│                              ┌───────────────┐                   │
│                              │ Scheduler     │                   │
│                              │ Event Loop    │                   │
│                              │ (binder)      │                   │
│                              └───────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
                                       ▲
                                       │ TCP/ZMQ
                                       │
┌─────────────────────────────────────────────────────────────────┐
│                      WORKER ADAPTER                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  NativeWorkerAdapter / ECSWorkerAdapter / etc.          │    │
│  │  - connects TO scheduler                                │    │
│  │  - sends WorkerAdapterHeartbeat                         │    │
│  │  - receives/executes commands                           │    │
│  │  - sends WorkerAdapterCommandResponse                   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                       │
│                          ▼ spawns                                │
│                    ┌──────────┐                                  │
│                    │ Workers  │ ──(connect separately)──► Scheduler
│                    └──────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. ScalingPolicy (renamed from ScalingController)

**Location**: `src/scaler/scheduler/controllers/policies/simple_policy/scaling/`

**Role**: Stateless advisor - receives snapshot, returns scaling advice

```python
class ScalingPolicy(ABC):
    @abstractmethod
    def get_scaling_advice(self, snapshot: ScalingSnapshot) -> ScalingAdvice:
        """Returns scaling decision based on current state."""
        raise NotImplementedError()


@dataclass
class ScalingSnapshot:
    tasks: Dict[TaskID, Task]
    workers: Dict[WorkerID, WorkerHeartbeat]
    worker_groups: Dict[WorkerGroupID, WorkerGroupInfo]
    adapter_info: WorkerAdapterInfo  # max_worker_groups, workers_per_group, etc.


@dataclass
class ScalingAdvice:
    action: Literal["none", "scale_up", "scale_down"]
    worker_group_id: Optional[WorkerGroupID] = None  # For scale_down
    capabilities: Optional[Dict] = None  # For scale_up with specific capabilities
```

### 2. WorkerAdapterController

**Location**: `src/scaler/scheduler/controllers/worker_adapter_controller.py`

**Role**: Owns state, executes commands, manages adapter connection

```python
class WorkerAdapterController(Looper):
    def __init__(self, scaling_policy: ScalingPolicy):
        self._scaling_policy = scaling_policy

        # State owned by this controller
        self._adapter_info: Optional[WorkerAdapterInfo] = None
        self._worker_groups: Dict[WorkerGroupID, List[WorkerID]] = {}
        self._pending_commands: Dict[CommandID, PendingCommand] = {}

        self._binder: Optional[AsyncBinder] = None
        self._adapter_id: Optional[WorkerAdapterID] = None

    def register(self, binder: AsyncBinder, ...):
        self._binder = binder

    async def on_heartbeat(self, adapter_id: WorkerAdapterID, heartbeat: WorkerAdapterHeartbeat):
        """Called by scheduler event loop when heartbeat received."""
        if self._adapter_id is None:
            self._adapter_id = adapter_id
            logging.info(f"WorkerAdapter {adapter_id!r} connected")

        self._adapter_info = WorkerAdapterInfo(
            max_worker_groups=heartbeat.max_worker_groups,
            workers_per_group=heartbeat.workers_per_group,
            base_capabilities=heartbeat.base_capabilities,
        )

        await self._binder.send(adapter_id, WorkerAdapterHeartbeatEcho.new_msg(...))

    async def on_command_response(self, adapter_id: WorkerAdapterID, response: WorkerAdapterCommandResponse):
        """Called by scheduler event loop when command response received."""
        pending = self._pending_commands.pop(response.command_id, None)
        if pending is None:
            logging.warning(f"Received response for unknown command: {response.command_id}")
            return

        if response.is_started():
            self._worker_groups[response.started.worker_group_id] = response.started.worker_ids
            logging.info(f"Started worker group: {response.started.worker_group_id}")
        elif response.is_shutdown():
            self._worker_groups.pop(pending.worker_group_id, None)
            logging.info(f"Shutdown worker group: {pending.worker_group_id}")
        elif response.is_error():
            logging.error(f"Command failed: {response.error.message}")

    async def routine(self):
        """Looper routine - called periodically."""
        if self._adapter_id is None or self._adapter_info is None:
            return  # No adapter connected yet

        snapshot = ScalingSnapshot(
            tasks=self._get_tasks(),
            workers=self._get_workers(),
            worker_groups=self._worker_groups,
            adapter_info=self._adapter_info,
        )

        advice = self._scaling_policy.get_scaling_advice(snapshot)

        if advice.action == "scale_up":
            await self._send_start_command(advice.capabilities)
        elif advice.action == "scale_down":
            await self._send_shutdown_command(advice.worker_group_id)

    async def _send_start_command(self, capabilities: Optional[Dict]):
        command_id = self._next_command_id()
        cmd = WorkerAdapterCommand.new_start_worker_group(command_id, capabilities)
        self._pending_commands[command_id] = PendingCommand(type="start")
        await self._binder.send(self._adapter_id, cmd)

    async def _send_shutdown_command(self, worker_group_id: WorkerGroupID):
        command_id = self._next_command_id()
        cmd = WorkerAdapterCommand.new_shutdown_worker_group(command_id, worker_group_id)
        self._pending_commands[command_id] = PendingCommand(type="shutdown", worker_group_id=worker_group_id)
        await self._binder.send(self._adapter_id, cmd)

    def get_status(self) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=self._worker_groups)
```

### 3. WorkerAdapter (refactored)

**Role**: Connects to scheduler, handles commands

```python
class NativeWorkerAdapter:
    def __init__(self, config: NativeWorkerAdapterConfig):
        self._scheduler_address = config.scheduler_address
        self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, Worker]] = {}
        self._connector: AsyncConnector = None  # Connects TO scheduler

    async def run(self):
        self._connector = create_connector(self._scheduler_address)

        # Start heartbeat loop
        asyncio.create_task(self._heartbeat_loop())

        # Main message loop
        while True:
            message = await self._connector.receive()
            await self._handle_message(message)

    async def _heartbeat_loop(self):
        while True:
            heartbeat = WorkerAdapterHeartbeat.new_msg(
                max_worker_groups=self._max_workers,
                workers_per_group=1,
                base_capabilities=self._capabilities,
            )
            await self._connector.send(heartbeat)
            await asyncio.sleep(self._heartbeat_interval)

    async def _handle_message(self, message: Message):
        if message.is_worker_adapter_heartbeat_echo():
            pass  # Update object storage address if needed
        elif message.is_worker_adapter_command():
            await self._handle_command(message.worker_adapter_command)

    async def _handle_command(self, cmd: WorkerAdapterCommand):
        try:
            if cmd.is_start_worker_group():
                worker_group_id, worker_ids = await self._start_worker_group()
                response = WorkerAdapterCommandResponse.new_started(
                    cmd.command_id, worker_group_id, worker_ids
                )
            elif cmd.is_shutdown_worker_group():
                await self._shutdown_worker_group(cmd.shutdown.worker_group_id)
                response = WorkerAdapterCommandResponse.new_shutdown(cmd.command_id)
        except Exception as e:
            response = WorkerAdapterCommandResponse.new_error(cmd.command_id, str(e))

        await self._connector.send(response)
```

## Message Flow

### 1. Startup & Registration

```
Adapter ────[TCP connect]────► Scheduler binder
Adapter ───WorkerAdapterHeartbeat───► Scheduler event loop
                                           │
                                           ▼
                      WorkerAdapterController.on_heartbeat()
                      - stores adapter_id, adapter_info
                                           │
                                           ▼
Adapter ◄──WorkerAdapterHeartbeatEcho──── Scheduler
```

### 2. Periodic Scaling (WorkerAdapterController.routine())

```
Build ScalingSnapshot from:
- tasks (from task manager)
- workers (from worker controller)
- worker_groups (owned by this controller)
- adapter_info (from heartbeat)
        │
        ▼
ScalingPolicy.get_scaling_advice(snapshot) → ScalingAdvice
        │
        ▼ (if scale_up or scale_down)
Adapter ◄──WorkerAdapterCommand──── WorkerAdapterController
```

### 3. Command Response

```
Adapter ───WorkerAdapterCommandResponse───► Scheduler event loop
                                                 │
                                                 ▼
                           WorkerAdapterController.on_command_response()
                           - updates worker_groups state
                           - logs result
```

## Cap'n Proto Messages

Add to `src/scaler/protocol/capnp/message.capnp`:

```capnp
struct WorkerAdapterHeartbeat {
    maxWorkerGroups @0 :UInt32;
    workersPerGroup @1 :UInt32;
    baseCapabilities @2 :List(CommonType.TaskCapability);
}

struct WorkerAdapterHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
}

struct WorkerAdapterCommand {
    commandId @0 :UInt64;
    union {
        startWorkerGroup @1 :StartWorkerGroup;
        shutdownWorkerGroup @2 :ShutdownWorkerGroup;
    }

    struct StartWorkerGroup {
        capabilities @0 :List(CommonType.TaskCapability);
    }
    struct ShutdownWorkerGroup {
        workerGroupId @0 :Data;
    }
}

struct WorkerAdapterCommandResponse {
    commandId @0 :UInt64;
    union {
        started @1 :WorkerGroupStarted;
        shutdown @2 :WorkerGroupShutdown;
        error @3 :CommandError;
    }

    struct WorkerGroupStarted {
        workerGroupId @0 :Data;
        workerIds @1 :List(Data);
    }
    struct WorkerGroupShutdown {}
    struct CommandError {
        code @0 :UInt16;
        message @1 :Text;
    }
}
```

Add to `Message` union:
```capnp
struct Message {
    union {
        # ... existing fields ...
        workerAdapterHeartbeat @25 :WorkerAdapterHeartbeat;
        workerAdapterHeartbeatEcho @26 :WorkerAdapterHeartbeatEcho;
        workerAdapterCommand @27 :WorkerAdapterCommand;
        workerAdapterCommandResponse @28 :WorkerAdapterCommandResponse;
    }
}
```

## Multi-Adapter Support (Future)

Current design supports single adapter per scheduler. For future multi-adapter support:

### Recommended Approach: Single Controller, Multiple Adapters

Similar to how `VanillaWorkerController` handles multiple workers:

```python
class WorkerAdapterController(Looper):
    def __init__(self, scaling_policy: ScalingPolicy):
        # Change from single adapter to dict
        self._adapters: Dict[WorkerAdapterID, AdapterState] = {}

@dataclass
class AdapterState:
    info: WorkerAdapterInfo
    worker_groups: Dict[WorkerGroupID, List[WorkerID]]
    pending_commands: Dict[CommandID, PendingCommand]
```

Extend `ScalingAdvice` to include adapter selection:

```python
@dataclass
class ScalingAdvice:
    action: Literal["none", "scale_up", "scale_down"]
    adapter_id: Optional[WorkerAdapterID] = None  # Which adapter
    worker_group_id: Optional[WorkerGroupID] = None
```

### Current Implementation Strategy

Design interface to be adapter-agnostic but implement for single adapter:

```python
async def on_heartbeat(self, adapter_id: WorkerAdapterID, heartbeat: WorkerAdapterHeartbeat):
    # For now, only accept one adapter
    if self._adapter_id is not None and self._adapter_id != adapter_id:
        logging.warning(f"Ignoring additional adapter {adapter_id}, already connected to {self._adapter_id}")
        return
    ...
```

## Open Items

1. **Snapshot source**: How does `WorkerAdapterController` get tasks/workers for snapshot?
   - Option A: Inject via `register()` (like `BalanceController` gets `task_controller`)
   - Option B: Share `InformationSnapshot` from `InformationController`

2. **Adapter timeout/disconnect handling**: Similar to worker timeout in `VanillaWorkerController.__clean_workers()`

3. **Command timeout**: What if adapter never responds to a command? Retry? Fail pending command after N seconds?

## Migration Path

1. Add new Cap'n Proto message types
2. Implement `WorkerAdapterController` in scheduler
3. Refactor `WorkerAdapter` implementations to connect TO scheduler instead of running HTTP server
4. Rename `ScalingController` to `ScalingPolicy`, update to return advice only
5. Update scheduler initialization to wire new components
6. Deprecate and remove `adapter_webhook_url` configuration
