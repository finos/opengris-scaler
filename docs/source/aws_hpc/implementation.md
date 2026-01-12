# AWS Batch Implementation Steps

## Phase 1: Protocol Extension (Week 1-2)

### Step 1.1: Add CommandTask Message Type
**Files to modify:**
- `src/scaler/protocol/python/message.py`
- `src/scaler/protocol/capnp/message.capnp`

**Tasks:**
1. Define `CommandTask` class with fields:
   - `task_id: TaskID`
   - `source: bytes`
   - `command: str`
   - `metadata: bytes`

2. Add serialization/deserialization methods
3. Update message factory methods
4. Add unit tests for new message type

**Acceptance Criteria:**
- [ ] CommandTask can be created and serialized
- [ ] CommandTask can be deserialized correctly
- [ ] All existing tests pass
- [ ] New message type tests pass

### Step 1.2: Update Scheduler Routing
**Files to modify:**
- `src/scaler/scheduler/scheduler.py`
- `src/scaler/scheduler/task_router.py`

**Tasks:**
1. Add CommandTask handling to message router
2. Route CommandTask to AWS Batch workers only
3. Maintain backward compatibility for Task messages
4. Add routing tests

**Acceptance Criteria:**
- [ ] Scheduler can route CommandTask messages
- [ ] Regular Task routing unchanged
- [ ] Worker type filtering works correctly
- [ ] Routing tests pass

## Phase 2: Client API Extension (Week 2-3)

### Step 2.1: Add submit_command Method
**Files to modify:**
- `src/scaler/client/client.py`
- `src/scaler/client/mixins.py`

**Tasks:**
1. Implement `submit_command(command: str, capabilities: Dict) -> Future`
2. Create CommandTask message from command string
3. Handle capabilities parameter
4. Return appropriate Future object
5. Add client-side validation

**Implementation:**
```python
def submit_command(self, command: str, capabilities: Optional[Dict] = None) -> Future:
    """Submit a command string for execution on AWS Batch."""
    task_id = TaskID.generate()
    source = self._get_source()
    metadata = self._serialize_capabilities(capabilities or {})
    
    command_task = CommandTask.new_msg(
        task_id=task_id,
        source=source,
        command=command,
        metadata=metadata
    )
    
    future = self._create_future(task_id)
    self._send_message(command_task)
    return future
```

**Acceptance Criteria:**
- [ ] submit_command method works correctly
- [ ] Capabilities are properly serialized
- [ ] Future is returned and tracked
- [ ] Client tests pass

### Step 2.2: Add Validation and Error Handling
**Tasks:**
1. Validate command string format
2. Validate required AWS Batch capabilities
3. Add meaningful error messages
4. Handle edge cases (empty command, invalid capabilities)

**Acceptance Criteria:**
- [ ] Invalid commands are rejected with clear errors
- [ ] Missing required capabilities are detected
- [ ] Error handling tests pass

## Phase 3: AWS Batch Worker Adapter (Week 3-5)

### Step 3.1: Extend Task Manager
**Files to modify:**
- `src/scaler/worker_adapter/aws_batch/task_manager.py`
- `src/scaler/worker_adapter/aws_batch/worker.py`

**Tasks:**
1. Add `on_command_task_new()` handler
2. Implement command type detection in `_submit_batch_job()`
3. Add AWS Batch parameter extraction
4. Update job submission logic

**Implementation:**
```python
async def on_command_task_new(self, command_task: CommandTask):
    """Handle new command task submission."""
    print(f"*** AWS BATCH: command_task_new() for {command_task.task_id.hex()[:8]} ***")
    
    # Store task and execute
    self._task_id_to_task[command_task.task_id] = command_task
    self._processing_task_ids.add(command_task.task_id)
    self._task_id_to_future[command_task.task_id] = await self._submit_batch_job(command_task)

async def _submit_batch_job(self, task_or_command):
    """Submit task to AWS Batch - handles both Task and CommandTask."""
    if isinstance(task_or_command, CommandTask):
        command = task_or_command.command
        capabilities = self._parse_capabilities(task_or_command.metadata)
    else:  # Regular Task
        command = await self._generate_python_command(task_or_command)
        capabilities = self._parse_capabilities(task_or_command.metadata)
    
    # Extract AWS Batch parameters
    batch_params = self._extract_batch_params(capabilities)
    
    # Submit job
    job_response = await self._submit_job_to_batch(command, batch_params)
    
    # Monitor and return future
    return await self._monitor_batch_job(job_response['jobId'])
```

**Acceptance Criteria:**
- [ ] CommandTask messages are handled correctly
- [ ] Both Task and CommandTask work in same worker
- [ ] AWS Batch parameters are extracted properly
- [ ] Job submission works (mock initially)

### Step 3.2: Implement AWS Batch Integration
**Tasks:**
1. Add boto3 AWS Batch client initialization
2. Implement job submission with proper parameters
3. Add job monitoring with status polling
4. Handle job completion and failure scenarios
5. Add multi-region support

**Implementation:**
```python
async def _submit_job_to_batch(self, command: str, params: BatchParams):
    """Submit job to AWS Batch service."""
    batch_client = self._get_batch_client(params.region)
    
    job_response = batch_client.submit_job(
        jobName=f"scaler-{uuid.uuid4().hex[:8]}",
        jobQueue=params.queue,
        jobDefinition=params.job_definition,
        containerOverrides={
            'command': command.split(),
            'vcpus': params.vcpus,
            'memory': params.memory
        }
    )
    
    job_id = job_response['jobId']
    self._task_id_to_batch_job_id[task_id] = job_id
    
    return job_response

async def _monitor_batch_job(self, job_id: str):
    """Monitor AWS Batch job until completion."""
    future = asyncio.Future()
    
    async def monitor():
        while True:
            job_detail = batch_client.describe_jobs(jobs=[job_id])
            status = job_detail['jobs'][0]['jobStatus']
            
            if status in ['SUCCEEDED', 'FAILED', 'TIMEOUT']:
                result = {
                    'batch_job_id': job_id,
                    'status': status,
                    'exit_code': job_detail['jobs'][0].get('exitCode')
                }
                future.set_result(result)
                break
                
            await asyncio.sleep(5)  # Poll every 5 seconds
    
    asyncio.create_task(monitor())
    return future
```

**Acceptance Criteria:**
- [ ] Jobs are submitted to AWS Batch successfully
- [ ] Job monitoring works correctly
- [ ] Job completion is detected
- [ ] Multi-region support works
- [ ] Error scenarios are handled

### Step 3.3: Add Command Generation
**Tasks:**
1. Implement Python command generation for regular tasks
2. Add support for built-in functions (math.sqrt, etc.)
3. Add support for user-defined functions
4. Handle argument serialization in commands

**Implementation:**
```python
async def _generate_python_command(self, task: Task) -> str:
    """Generate Python command from task function and arguments."""
    # Deserialize function and arguments
    function = await self._deserialize_function(task)
    args = await self._deserialize_arguments(task)
    
    if hasattr(function, '__module__') and function.__module__ in BUILTIN_MODULES:
        # Built-in function: import math; print(math.sqrt(16))
        module = function.__module__
        func_name = function.__name__
        args_str = ', '.join(repr(arg) for arg in args)
        return f"python -c 'import {module}; print({module}.{func_name}({args_str}))'"
    else:
        # User-defined function: serialize and execute
        func_source = inspect.getsource(function)
        args_str = ', '.join(repr(arg) for arg in args)
        return f"python -c '{func_source}; print({function.__name__}({args_str}))'"
```

**Acceptance Criteria:**
- [ ] Built-in functions generate correct commands
- [ ] User-defined functions work
- [ ] Arguments are properly serialized
- [ ] Generated commands execute correctly

## Phase 4: Integration and Testing (Week 5-6)

### Step 4.1: End-to-End Integration
**Tasks:**
1. Update worker message routing
2. Add capability-based worker selection
3. Test complete flow: Client → Scheduler → AWS Batch Worker → AWS Batch
4. Add integration tests

**Acceptance Criteria:**
- [ ] Complete flow works end-to-end
- [ ] Both submit() and submit_command() work
- [ ] AWS Batch jobs are created and monitored
- [ ] Results are returned correctly

### Step 4.2: Error Handling and Edge Cases
**Tasks:**
1. Handle AWS Batch service errors
2. Add retry logic for transient failures
3. Handle job timeouts and cancellations
4. Add comprehensive error logging

**Acceptance Criteria:**
- [ ] Service errors are handled gracefully
- [ ] Retry logic works correctly
- [ ] Timeouts and cancellations work
- [ ] Error messages are informative

### Step 4.3: Performance Optimization
**Tasks:**
1. Optimize job monitoring polling intervals
2. Add connection pooling for AWS clients
3. Implement efficient job status caching
4. Add metrics and monitoring

**Acceptance Criteria:**
- [ ] Job monitoring is efficient
- [ ] AWS API calls are optimized
- [ ] Performance metrics are available
- [ ] System scales under load

## Phase 5: Documentation and Deployment (Week 6-7)

### Step 5.1: Documentation
**Tasks:**
1. Write user guide for AWS Batch integration
2. Document capability parameters
3. Add code examples and tutorials
4. Update API documentation

### Step 5.2: Deployment Preparation
**Tasks:**
1. Create deployment scripts
2. Add configuration templates
3. Write operational runbooks
4. Prepare monitoring dashboards

## Implementation Checklist

### Protocol Changes
- [ ] CommandTask message type added
- [ ] Message serialization/deserialization works
- [ ] Scheduler routing updated
- [ ] Protocol tests pass

### Client API
- [ ] submit_command() method implemented
- [ ] Capabilities handling works
- [ ] Client validation added
- [ ] Client tests pass

### AWS Batch Worker
- [ ] CommandTask handler added
- [ ] AWS Batch integration works
- [ ] Job monitoring implemented
- [ ] Multi-region support added
- [ ] Error handling complete

### Integration
- [ ] End-to-end flow works
- [ ] Both task types supported
- [ ] Performance optimized
- [ ] Documentation complete

### Testing
- [ ] Unit tests for all components
- [ ] Integration tests pass
- [ ] Performance tests pass
- [ ] Error scenario tests pass

## Risk Mitigation

### Technical Risks
1. **AWS Batch API limits**: Implement rate limiting and retry logic
2. **Job monitoring overhead**: Use efficient polling strategies
3. **Command generation complexity**: Start with simple cases, expand gradually
4. **Protocol compatibility**: Maintain backward compatibility throughout

### Operational Risks
1. **AWS service availability**: Add fallback mechanisms
2. **Cost management**: Implement job resource limits
3. **Security concerns**: Follow AWS security best practices
4. **Monitoring gaps**: Add comprehensive logging and metrics

## Success Metrics

### Functional Metrics
- [ ] 100% of existing tests pass
- [ ] New functionality tests pass
- [ ] End-to-end scenarios work
- [ ] Error scenarios handled correctly

### Performance Metrics
- [ ] Job submission latency < 2 seconds
- [ ] Job monitoring overhead < 5% CPU
- [ ] Memory usage within acceptable limits
- [ ] Concurrent job handling works

### Quality Metrics
- [ ] Code coverage > 80%
- [ ] Documentation complete
- [ ] Security review passed
- [ ] Performance review passed