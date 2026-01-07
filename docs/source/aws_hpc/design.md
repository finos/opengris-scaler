# AWS Batch Integration Design

## Overview

This document outlines the design for integrating AWS Batch with OpenGRIS Scaler, enabling distributed task execution on AWS Batch compute environments while maintaining the existing Scaler client API.

## Architecture Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Scaler Client │    │   Scheduler     │    │ AWS Batch       │
│                 │    │                 │    │ Worker Adapter  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ submit()        │───▶│ Task Router     │───▶│ TaskManager     │
│ submit_command()│    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │   AWS Batch     │
                                              │   Service       │
                                              ├─────────────────┤
                                              │ Job Queue       │
                                              │ Job Definition  │
                                              │ Compute Env     │
                                              └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │   EC2 Instance  │
                                              │   (Container)   │
                                              ├─────────────────┤
                                              │ python -c "..." │
                                              │ bash script.sh  │
                                              └─────────────────┘
```

## Message Flow Diagram

```
Client                Scheduler              AWS Batch Worker
  │                      │                         │
  │ submit(func, args)   │                         │
  ├─────────────────────▶│                         │
  │                      │ Task                    │
  │                      ├────────────────────────▶│
  │                      │                         │ on_task_new()
  │                      │                         ├─────────────┐
  │                      │                         │ Generate    │
  │                      │                         │ python -c   │
  │                      │                         │ command     │
  │                      │                         ◀─────────────┘
  │                      │                         │
  │                      │                         │ batch.submit_job()
  │                      │                         ├─────────────┐
  │                      │                         │ Monitor     │
  │                      │                         │ job status  │
  │                      │                         ◀─────────────┘
  │                      │ TaskResult              │
  │                      ◀────────────────────────┤
  │ Future.result()      │                         │
  ◀─────────────────────┤                         │
  │                      │                         │

Client                Scheduler              AWS Batch Worker
  │                      │                         │
  │ submit_command(cmd)  │                         │
  ├─────────────────────▶│                         │
  │                      │ CommandTask             │
  │                      ├────────────────────────▶│
  │                      │                         │ on_command_task_new()
  │                      │                         ├─────────────┐
  │                      │                         │ Use command │
  │                      │                         │ directly    │
  │                      │                         ◀─────────────┘
  │                      │                         │
  │                      │                         │ batch.submit_job()
  │                      │                         ├─────────────┐
  │                      │                         │ Monitor     │
  │                      │                         │ job status  │
  │                      │                         ◀─────────────┘
  │                      │ TaskResult              │
  │                      ◀────────────────────────┤
  │ Future.result()      │                         │
  ◀─────────────────────┤                         │
```

## Protocol Extension

### New Message Types

#### CommandTask Message
```python
class CommandTask:
    task_id: TaskID
    source: bytes
    command: str
    metadata: bytes  # Contains capabilities and flags
```

### Message Routing Strategy

```
┌─────────────────┐
│   Task Router   │
├─────────────────┤
│ if worker_type  │
│   == "aws_batch"│
│   route Task &  │
│   CommandTask   │
│ else            │
│   route Task    │
│   only          │
└─────────────────┘
```

## Task Processing Flow

### Function-Based Tasks (submit)
```
Task → Deserialize Function & Args → Generate Python Command → AWS Batch Job
```

### Command-Based Tasks (submit_command)
```
CommandTask → Extract Command String → AWS Batch Job
```

### Unified Batch Submission
```python
def _submit_batch_job(task_or_command):
    # 1. Extract AWS Batch parameters from capabilities
    params = extract_batch_params(task_or_command.metadata)
    
    # 2. Generate command string
    if isinstance(task_or_command, CommandTask):
        command = task_or_command.command
    else:  # Task
        command = generate_python_command(function, args)
    
    # 3. Submit to AWS Batch
    job_response = batch_client.submit_job(
        jobName=f"scaler-{task_id[:8]}",
        jobQueue=params.queue,
        jobDefinition=params.job_def,
        containerOverrides={
            'command': command.split(),
            'vcpus': params.vcpus,
            'memory': params.memory
        }
    )
    
    # 4. Monitor job and return future
    return monitor_job(job_response['jobId'])
```

## Capability System Integration

### AWS Batch Parameters via Capabilities
```python
# Function-based task with AWS Batch parameters
future = client.submit_verbose(
    math.sqrt, 
    args=(16,),
    capabilities={
        "aws_region": "us-east-1",
        "batch_queue": "my-compute-queue",
        "batch_job_def": "python-job-def",
        "vcpus": 2,
        "memory": 4096
    }
)

# Command-based task with AWS Batch parameters
future = client.submit_command(
    "bash /work/process_data.sh --input /data/file.csv",
    capabilities={
        "aws_region": "us-west-2", 
        "batch_queue": "gpu-queue",
        "batch_job_def": "ml-job-def",
        "vcpus": 4,
        "memory": 8192
    }
)
```

## Error Handling Strategy

### Job Failure Scenarios
1. **Job Submission Failure**: Return failed future immediately
2. **Job Execution Failure**: Monitor job status, return failure when detected
3. **Job Timeout**: Cancel job and return timeout error
4. **Network Issues**: Retry with exponential backoff

### Error Mapping
```python
AWS_BATCH_STATUS_TO_SCALER = {
    'SUCCEEDED': TaskResultType.Success,
    'FAILED': TaskResultType.Failed,
    'TIMEOUT': TaskResultType.Failed,
    'CANCELLED': TaskResultType.Canceled
}
```

## Multi-Region Support

### Region-Specific Batch Clients
```python
class AWSBatchTaskManager:
    def __init__(self):
        self._batch_clients: Dict[str, boto3.client] = {}
    
    def get_batch_client(self, region: str):
        if region not in self._batch_clients:
            self._batch_clients[region] = boto3.client('batch', region_name=region)
        return self._batch_clients[region]
```

## Security Considerations

### IAM Permissions Required
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs",
                "batch:CancelJob",
                "batch:TerminateJob"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:GetLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:/aws/batch/job"
        }
    ]
}
```

### Container Security
- Use least-privilege container images
- Avoid embedding AWS credentials in containers
- Use IAM roles for service accounts when possible

## Performance Considerations

### Job Monitoring Optimization
- Batch job status polling with exponential backoff
- Maximum concurrent monitoring tasks limit
- Efficient job ID to task ID mapping

### Resource Management
- Semaphore-based concurrency control
- Job queue capacity monitoring
- Automatic scaling based on queue depth

## Backward Compatibility

### Existing API Preservation
- `client.submit()` continues to work unchanged
- No breaking changes to core Scaler protocol
- AWS Batch functionality is opt-in via capabilities

### Migration Path
1. **Phase 1**: Deploy AWS Batch worker adapter alongside existing workers
2. **Phase 2**: Gradually migrate workloads using capabilities
3. **Phase 3**: Full AWS Batch adoption for suitable workloads