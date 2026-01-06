# Batch Adapter Simplification Plan

## Status: Proposed (for future implementation)

## Problem

Current batch-style adapters (AWS Batch, future Kubernetes, Slurm) require:
1. Object storage access to fetch function/arguments
2. Complex protocol handling
3. ~400 lines of adapter code

## Proposed Solution: Base Class Abstraction (No Scheduler Changes)

Create a base class that handles all the complexity, so new backends only implement job submission.

### Current Implementation (Complex)
```
Client → Scheduler → Object Storage → Task(references) → Adapter → Object Storage → Payload → Backend
                                                              ↑
                                                    ~400 lines of code
```

### Proposed Implementation (Simplified)
```
Client → Scheduler → Object Storage → Task(references) → BatchWorkerAdapterBase → Payload → Backend
                                                              ↑                        ↑
                                                    Base class handles this    Subclass: ~50 lines
```

## Design

### 1. Base Class (Handles All Complexity)

```python
class BatchWorkerAdapterBase:
    """
    Base class for batch-style worker adapters.
    Handles object storage fetching, payload serialization, result handling.
    Subclasses only implement job submission/polling.
    """
    
    def __init__(self, s3_bucket: str, s3_prefix: str, max_concurrent_jobs: int = 100):
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._pending_jobs: Dict[str, TaskID] = {}  # job_id -> task_id
        
    async def on_task(self, task: Task):
        """Called by worker when task received. Fetches payload and submits job."""
        await self._semaphore.acquire()
        
        try:
            # Fetch function and arguments from object storage
            func_bytes = await self._connector_storage.get_object(task.func_object_id)
            func = cloudpickle.loads(func_bytes)
            
            arguments = []
            for arg in task.function_args:
                if isinstance(arg, TaskID):
                    raise ValueError("Task dependencies not supported in batch adapters")
                arg_bytes = await self._connector_storage.get_object(arg)
                arguments.append(cloudpickle.loads(arg_bytes))
            
            # Create payload
            payload = cloudpickle.dumps({
                "task_id": task.task_id.hex(),
                "function": func,
                "arguments": arguments,
            })
            
            # Compress if beneficial
            if len(payload) > 4096:
                payload = gzip.compress(payload)
                compressed = True
            else:
                compressed = False
            
            # Submit to backend (implemented by subclass)
            job_id = await self.submit_job(task.task_id.hex(), payload, compressed)
            self._pending_jobs[job_id] = task.task_id
            
        except Exception as e:
            self._semaphore.release()
            await self._send_task_failed(task.task_id, e)
    
    async def submit_job(self, task_id: str, payload: bytes, compressed: bool) -> str:
        """
        Submit job to backend. Override in subclass.
        
        Args:
            task_id: Scaler task ID (hex string)
            payload: Serialized function + arguments (may be gzip compressed)
            compressed: Whether payload is compressed
            
        Returns:
            Backend job ID for tracking
        """
        raise NotImplementedError
    
    async def poll_jobs(self) -> List[Tuple[str, str, Optional[bytes]]]:
        """
        Poll backend for job status. Override in subclass.
        
        Returns:
            List of (job_id, status, result_bytes) where:
            - status: "succeeded", "failed", "running"
            - result_bytes: Result data if succeeded, error message if failed, None if running
        """
        raise NotImplementedError
    
    async def cancel_job(self, job_id: str):
        """Cancel a job. Override in subclass."""
        raise NotImplementedError
    
    # Base class handles result routing back to scheduler
    async def routine(self):
        """Poll for completions and route results."""
        results = await self.poll_jobs()
        for job_id, status, result_bytes in results:
            task_id = self._pending_jobs.pop(job_id, None)
            if task_id is None:
                continue
            
            if status == "succeeded":
                await self._send_task_succeeded(task_id, result_bytes)
            elif status == "failed":
                await self._send_task_failed(task_id, RuntimeError(result_bytes.decode() if result_bytes else "Unknown error"))
            
            self._semaphore.release()
```

### 2. AWS Batch Implementation (~50 lines)

```python
class AWSBatchAdapter(BatchWorkerAdapterBase):
    def __init__(self, job_queue: str, job_definition: str, **kwargs):
        super().__init__(**kwargs)
        self._batch = boto3.client("batch")
        self._job_queue = job_queue
        self._job_definition = job_definition
    
    async def submit_job(self, task_id: str, payload: bytes, compressed: bool) -> str:
        response = self._batch.submit_job(
            jobName=f"scaler-{task_id[:12]}",
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            parameters={
                "payload": base64.b64encode(payload).decode(),
                "compressed": "1" if compressed else "0",
                "s3_bucket": self._s3_bucket,
                "s3_prefix": self._s3_prefix,
            },
        )
        return response["jobId"]
    
    async def poll_jobs(self):
        job_ids = list(self._pending_jobs.keys())
        if not job_ids:
            return []
        
        results = []
        response = self._batch.describe_jobs(jobs=job_ids)
        for job in response["jobs"]:
            if job["status"] == "SUCCEEDED":
                result = self._fetch_result_from_s3(job["jobId"])
                results.append((job["jobId"], "succeeded", result))
            elif job["status"] == "FAILED":
                results.append((job["jobId"], "failed", job.get("statusReason", "").encode()))
        return results
    
    async def cancel_job(self, job_id: str):
        self._batch.terminate_job(jobId=job_id, reason="Canceled")
```

### 3. Kubernetes Implementation (~50 lines)

```python
class KubernetesAdapter(BatchWorkerAdapterBase):
    def __init__(self, namespace: str = "default", **kwargs):
        super().__init__(**kwargs)
        self._k8s = kubernetes.client.BatchV1Api()
        self._namespace = namespace
    
    async def submit_job(self, task_id: str, payload: bytes, compressed: bool) -> str:
        job = kubernetes.client.V1Job(
            metadata={"name": f"scaler-{task_id[:12]}"},
            spec=kubernetes.client.V1JobSpec(
                template=kubernetes.client.V1PodTemplateSpec(
                    spec=kubernetes.client.V1PodSpec(
                        containers=[kubernetes.client.V1Container(
                            name="worker",
                            image="scaler-batch-runner:latest",
                            env=[
                                {"name": "PAYLOAD", "value": base64.b64encode(payload).decode()},
                                {"name": "COMPRESSED", "value": "1" if compressed else "0"},
                            ],
                        )],
                        restart_policy="Never",
                    )
                )
            )
        )
        self._k8s.create_namespaced_job(self._namespace, job)
        return f"scaler-{task_id[:12]}"
    
    async def poll_jobs(self):
        # Similar pattern - check job status, fetch results
        pass
    
    async def cancel_job(self, job_id: str):
        self._k8s.delete_namespaced_job(job_id, self._namespace)
```

### 4. Slurm Implementation (~50 lines)

```python
class SlurmAdapter(BatchWorkerAdapterBase):
    async def submit_job(self, task_id: str, payload: bytes, compressed: bool) -> str:
        # Write payload to shared filesystem
        payload_path = f"/scratch/scaler/{task_id}.pkl"
        with open(payload_path, "wb") as f:
            f.write(payload)
        
        # Submit via sbatch
        result = subprocess.run([
            "sbatch", "--parsable",
            "--job-name", f"scaler-{task_id[:12]}",
            "--wrap", f"python /opt/scaler/batch_runner.py --payload {payload_path}"
        ], capture_output=True)
        
        return result.stdout.decode().strip()  # Slurm job ID
    
    async def poll_jobs(self):
        # Use sacct to check job status
        pass
    
    async def cancel_job(self, job_id: str):
        subprocess.run(["scancel", job_id])
```

## Benefits

| Aspect | Current | With Base Class |
|--------|---------|-----------------|
| New backend implementation | ~400 lines | ~50 lines |
| Time to implement | Days | Hours |
| Object storage handling | Each adapter | Base class |
| Result routing | Each adapter | Base class |
| Error handling | Each adapter | Base class |
| Scheduler changes | None | None |

## Implementation Steps

1. Create `BatchWorkerAdapterBase` in `src/scaler/worker_adapter/base.py`
2. Refactor `AWSBatchWorkerAdapter` to extend base class
3. Test with existing AWS Batch setup
4. Document pattern for new backends
5. (Optional) Implement Kubernetes adapter as example

## Shared Batch Job Runner

All backends can use the same `batch_job_runner.py`:

```python
# batch_job_runner.py - Universal runner for all batch backends
import cloudpickle, gzip, base64, os

payload = base64.b64decode(os.environ["PAYLOAD"])
if os.environ.get("COMPRESSED") == "1":
    payload = gzip.decompress(payload)

task_data = cloudpickle.loads(payload)
result = task_data["function"](*task_data["arguments"])

# Store result (S3, shared filesystem, etc. based on config)
store_result(result)
```

## Future Enhancement: Scheduler-Side Optimization

If we later want to optimize further, the scheduler could:
1. Detect `direct_payload` capability in worker registration
2. Send embedded payload directly (skip object storage for small payloads)
3. This would eliminate the object storage round-trip entirely

But the base class approach gives 80% of the benefit with 0% scheduler changes.
