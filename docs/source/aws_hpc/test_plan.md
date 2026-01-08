# AWS Batch Integration Test Plan

## Test Strategy Overview

This document outlines the comprehensive testing strategy for AWS Batch integration with OpenGRIS Scaler, covering unit tests, integration tests, performance tests, and end-to-end validation.

## Test Environment Setup

### Prerequisites
```bash
# AWS CLI configured with appropriate permissions
aws configure

# Test AWS Batch resources
aws batch create-compute-environment --compute-environment-name scaler-test-env
aws batch create-job-queue --job-queue-name scaler-test-queue
aws batch register-job-definition --job-definition-name scaler-test-job-def

# Test containers
docker build -t scaler-test-container .
```

### Test Infrastructure
- **AWS Account**: Dedicated test account with Batch resources
- **Compute Environment**: Small EC2 instances for cost efficiency
- **Job Queue**: Test queue with limited capacity
- **Job Definition**: Python 3.9 container with required dependencies

## Unit Tests

### Protocol Layer Tests

#### CommandTask Message Tests
**File**: `tests/unit/protocol/test_command_task.py`

```python
class TestCommandTask:
    def test_command_task_creation(self):
        """Test CommandTask message creation."""
        task_id = TaskID.generate()
        command = "python -c 'print(42)'"
        
        command_task = CommandTask.new_msg(
            task_id=task_id,
            source=b"test_source",
            command=command,
            metadata=b"test_metadata"
        )
        
        assert command_task.task_id == task_id
        assert command_task.command == command
        assert command_task.source == b"test_source"
        assert command_task.metadata == b"test_metadata"
    
    def test_command_task_serialization(self):
        """Test CommandTask serialization/deserialization."""
        original = CommandTask.new_msg(
            task_id=TaskID.generate(),
            source=b"test",
            command="echo hello",
            metadata=b"meta"
        )
        
        serialized = original.serialize()
        deserialized = CommandTask.deserialize(serialized)
        
        assert deserialized.task_id == original.task_id
        assert deserialized.command == original.command
        assert deserialized.source == original.source
        assert deserialized.metadata == original.metadata
    
    def test_command_task_validation(self):
        """Test CommandTask validation."""
        with pytest.raises(ValueError):
            CommandTask.new_msg(
                task_id=None,  # Invalid
                source=b"test",
                command="echo hello",
                metadata=b"meta"
            )
```

**Test Cases:**
- [ ] CommandTask creation with valid parameters
- [ ] CommandTask serialization/deserialization
- [ ] CommandTask validation (invalid parameters)
- [ ] CommandTask equality comparison
- [ ] CommandTask string representation

### Client API Tests

#### submit_command Method Tests
**File**: `tests/unit/client/test_submit_command.py`

```python
class TestSubmitCommand:
    def test_submit_command_basic(self):
        """Test basic submit_command functionality."""
        client = Client(address="tcp://127.0.0.1:2345")
        
        future = client.submit_command("echo hello world")
        
        assert isinstance(future, Future)
        assert future.task_id is not None
    
    def test_submit_command_with_capabilities(self):
        """Test submit_command with AWS Batch capabilities."""
        client = Client(address="tcp://127.0.0.1:2345")
        
        capabilities = {
            "aws_region": "us-east-1",
            "batch_queue": "test-queue",
            "batch_job_def": "test-job-def",
            "vcpus": 2,
            "memory": 4096
        }
        
        future = client.submit_command("python -c 'print(42)'", capabilities=capabilities)
        
        assert isinstance(future, Future)
        # Verify capabilities are properly encoded in the message
    
    def test_submit_command_validation(self):
        """Test submit_command input validation."""
        client = Client(address="tcp://127.0.0.1:2345")
        
        # Empty command should raise error
        with pytest.raises(ValueError):
            client.submit_command("")
        
        # Invalid capabilities should raise error
        with pytest.raises(ValueError):
            client.submit_command("echo hello", capabilities={"invalid": "param"})
```

**Test Cases:**
- [ ] Basic command submission
- [ ] Command submission with capabilities
- [ ] Input validation (empty command, invalid capabilities)
- [ ] Future object creation and tracking
- [ ] Error handling for malformed commands

### AWS Batch Task Manager Tests

#### Command Processing Tests
**File**: `tests/unit/worker_adapter/test_aws_batch_task_manager.py`

```python
class TestAWSBatchTaskManager:
    @pytest.fixture
    def task_manager(self):
        return AWSBatchTaskManager(
            base_concurrency=4,
            job_queue="test-queue",
            job_definition="test-job-def",
            aws_region="us-east-1"
        )
    
    def test_command_task_handling(self, task_manager):
        """Test CommandTask message handling."""
        command_task = CommandTask.new_msg(
            task_id=TaskID.generate(),
            source=b"test",
            command="echo hello",
            metadata=b"{}"
        )
        
        # Mock AWS Batch client
        with patch.object(task_manager, '_batch_client') as mock_client:
            mock_client.submit_job.return_value = {'jobId': 'test-job-123'}
            
            future = asyncio.run(task_manager.on_command_task_new(command_task))
            
            assert isinstance(future, asyncio.Future)
            mock_client.submit_job.assert_called_once()
    
    def test_python_command_generation(self, task_manager):
        """Test Python command generation from Task."""
        # Mock task with math.sqrt function
        task = create_mock_task_with_function(math.sqrt, [16])
        
        command = asyncio.run(task_manager._generate_python_command(task))
        
        expected = "python -c 'import math; print(math.sqrt(16))'"
        assert command == expected
    
    def test_batch_parameter_extraction(self, task_manager):
        """Test AWS Batch parameter extraction from capabilities."""
        capabilities = {
            "aws_region": "us-west-2",
            "batch_queue": "gpu-queue",
            "batch_job_def": "ml-job-def",
            "vcpus": 4,
            "memory": 8192
        }
        
        params = task_manager._extract_batch_params(capabilities)
        
        assert params.region == "us-west-2"
        assert params.queue == "gpu-queue"
        assert params.job_definition == "ml-job-def"
        assert params.vcpus == 4
        assert params.memory == 8192
```

**Test Cases:**
- [ ] CommandTask message handling
- [ ] Regular Task message handling
- [ ] Python command generation (built-in functions)
- [ ] Python command generation (user-defined functions)
- [ ] AWS Batch parameter extraction
- [ ] Job submission (mocked)
- [ ] Job monitoring (mocked)
- [ ] Error handling scenarios

## Integration Tests

### End-to-End Flow Tests

#### Complete Workflow Tests
**File**: `tests/integration/test_aws_batch_e2e.py`

```python
class TestAWSBatchE2E:
    @pytest.fixture(scope="class")
    def test_cluster(self):
        """Set up test cluster with AWS Batch worker."""
        # Start scheduler
        scheduler = start_test_scheduler()
        
        # Start AWS Batch worker adapter
        worker_adapter = start_aws_batch_worker_adapter()
        
        yield scheduler, worker_adapter
        
        # Cleanup
        scheduler.shutdown()
        worker_adapter.shutdown()
    
    def test_function_task_e2e(self, test_cluster):
        """Test complete flow for function-based task."""
        scheduler, worker_adapter = test_cluster
        
        with Client(address=scheduler.address) as client:
            # Submit function task with AWS Batch capabilities
            future = client.submit_verbose(
                math.sqrt,
                args=(16,),
                capabilities={
                    "aws_region": "us-east-1",
                    "batch_queue": "test-queue",
                    "batch_job_def": "test-job-def"
                }
            )
            
            # Wait for result
            result = future.result(timeout=300)  # 5 minute timeout
            
            # Verify result structure (job metadata, not actual result)
            assert "batch_job_id" in result
            assert "status" in result
            assert result["status"] in ["SUCCEEDED", "FAILED"]
    
    def test_command_task_e2e(self, test_cluster):
        """Test complete flow for command-based task."""
        scheduler, worker_adapter = test_cluster
        
        with Client(address=scheduler.address) as client:
            # Submit command task
            future = client.submit_command(
                "python -c 'import math; print(math.sqrt(25))'",
                capabilities={
                    "aws_region": "us-east-1",
                    "batch_queue": "test-queue",
                    "batch_job_def": "test-job-def"
                }
            )
            
            # Wait for result
            result = future.result(timeout=300)
            
            # Verify job completion
            assert "batch_job_id" in result
            assert result["status"] == "SUCCEEDED"
    
    def test_multiple_tasks_concurrent(self, test_cluster):
        """Test concurrent task execution."""
        scheduler, worker_adapter = test_cluster
        
        with Client(address=scheduler.address) as client:
            # Submit multiple tasks
            futures = []
            for i in range(5):
                future = client.submit_command(
                    f"echo 'Task {i}'",
                    capabilities={
                        "aws_region": "us-east-1",
                        "batch_queue": "test-queue",
                        "batch_job_def": "test-job-def"
                    }
                )
                futures.append(future)
            
            # Wait for all results
            results = [f.result(timeout=300) for f in futures]
            
            # Verify all completed
            assert len(results) == 5
            assert all(r["status"] == "SUCCEEDED" for r in results)
```

**Test Cases:**
- [ ] Function-based task end-to-end
- [ ] Command-based task end-to-end
- [ ] Multiple concurrent tasks
- [ ] Task cancellation
- [ ] Error scenarios (invalid queue, job definition)
- [ ] Multi-region task execution

### AWS Batch Service Integration Tests

#### Real AWS Batch Tests
**File**: `tests/integration/test_aws_batch_service.py`

```python
class TestAWSBatchService:
    @pytest.fixture
    def batch_client(self):
        return boto3.client('batch', region_name='us-east-1')
    
    def test_job_submission(self, batch_client):
        """Test actual job submission to AWS Batch."""
        job_response = batch_client.submit_job(
            jobName='scaler-test-job',
            jobQueue='scaler-test-queue',
            jobDefinition='scaler-test-job-def',
            containerOverrides={
                'command': ['python', '-c', 'print("Hello from Batch")'],
                'vcpus': 1,
                'memory': 512
            }
        )
        
        job_id = job_response['jobId']
        assert job_id is not None
        
        # Monitor job until completion
        job_completed = wait_for_job_completion(batch_client, job_id, timeout=300)
        assert job_completed
        
        # Verify job succeeded
        job_detail = batch_client.describe_jobs(jobs=[job_id])
        assert job_detail['jobs'][0]['jobStatus'] == 'SUCCEEDED'
    
    def test_job_monitoring(self, batch_client):
        """Test job status monitoring."""
        # Submit a long-running job
        job_response = batch_client.submit_job(
            jobName='scaler-monitor-test',
            jobQueue='scaler-test-queue',
            jobDefinition='scaler-test-job-def',
            containerOverrides={
                'command': ['sleep', '30'],
                'vcpus': 1,
                'memory': 512
            }
        )
        
        job_id = job_response['jobId']
        
        # Monitor status changes
        statuses = []
        start_time = time.time()
        
        while time.time() - start_time < 60:  # 1 minute timeout
            job_detail = batch_client.describe_jobs(jobs=[job_id])
            status = job_detail['jobs'][0]['jobStatus']
            
            if status not in statuses:
                statuses.append(status)
            
            if status in ['SUCCEEDED', 'FAILED']:
                break
                
            time.sleep(5)
        
        # Verify status progression
        assert 'SUBMITTED' in statuses or 'PENDING' in statuses
        assert 'RUNNING' in statuses
        assert 'SUCCEEDED' in statuses
```

**Test Cases:**
- [ ] Job submission to real AWS Batch
- [ ] Job status monitoring
- [ ] Job cancellation
- [ ] Job failure scenarios
- [ ] Resource limit testing
- [ ] Multi-region job submission

## Performance Tests

### Load Testing

#### Concurrent Job Submission Tests
**File**: `tests/performance/test_aws_batch_load.py`

```python
class TestAWSBatchLoad:
    def test_concurrent_job_submission(self):
        """Test system under concurrent job load."""
        num_tasks = 50
        concurrent_limit = 10
        
        with Client(address="tcp://127.0.0.1:2345") as client:
            # Submit tasks with concurrency control
            semaphore = asyncio.Semaphore(concurrent_limit)
            
            async def submit_task(task_id):
                async with semaphore:
                    future = client.submit_command(
                        f"echo 'Task {task_id}'",
                        capabilities={
                            "aws_region": "us-east-1",
                            "batch_queue": "test-queue",
                            "batch_job_def": "test-job-def"
                        }
                    )
                    return await future
            
            # Measure performance
            start_time = time.time()
            
            tasks = [submit_task(i) for i in range(num_tasks)]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Performance assertions
            assert len(results) == num_tasks
            assert duration < 600  # Should complete within 10 minutes
            assert all(r["status"] == "SUCCEEDED" for r in results)
            
            # Calculate throughput
            throughput = num_tasks / duration
            print(f"Throughput: {throughput:.2f} tasks/second")
    
    def test_memory_usage_under_load(self):
        """Test memory usage during high load."""
        import psutil
        
        process = psutil.Process()
        initial_memory = process.memory_info().rss
        
        # Submit many tasks
        with Client(address="tcp://127.0.0.1:2345") as client:
            futures = []
            for i in range(100):
                future = client.submit_command(f"echo {i}")
                futures.append(future)
            
            # Monitor memory during execution
            peak_memory = initial_memory
            for future in futures:
                current_memory = process.memory_info().rss
                peak_memory = max(peak_memory, current_memory)
                future.result(timeout=60)
        
        # Memory should not grow excessively
        memory_growth = peak_memory - initial_memory
        memory_growth_mb = memory_growth / (1024 * 1024)
        
        assert memory_growth_mb < 500  # Less than 500MB growth
```

**Test Cases:**
- [ ] Concurrent job submission (10, 50, 100 tasks)
- [ ] Memory usage under load
- [ ] CPU usage monitoring
- [ ] Network bandwidth utilization
- [ ] AWS API rate limiting behavior
- [ ] Job queue capacity limits

### Scalability Tests

#### Multi-Region Performance Tests
**File**: `tests/performance/test_multi_region.py`

```python
class TestMultiRegionPerformance:
    def test_cross_region_latency(self):
        """Test latency for cross-region job submission."""
        regions = ["us-east-1", "us-west-2", "eu-west-1"]
        
        latencies = {}
        
        for region in regions:
            start_time = time.time()
            
            with Client(address="tcp://127.0.0.1:2345") as client:
                future = client.submit_command(
                    "echo 'Hello from {}'".format(region),
                    capabilities={
                        "aws_region": region,
                        "batch_queue": f"test-queue-{region}",
                        "batch_job_def": f"test-job-def-{region}"
                    }
                )
                
                result = future.result(timeout=300)
            
            end_time = time.time()
            latencies[region] = end_time - start_time
        
        # Verify reasonable latencies
        for region, latency in latencies.items():
            assert latency < 180  # Less than 3 minutes per region
            print(f"{region}: {latency:.2f}s")
```

## Error Scenario Tests

### Failure Mode Tests

#### AWS Service Failure Tests
**File**: `tests/error/test_aws_batch_failures.py`

```python
class TestAWSBatchFailures:
    def test_invalid_job_queue(self):
        """Test handling of invalid job queue."""
        with Client(address="tcp://127.0.0.1:2345") as client:
            future = client.submit_command(
                "echo hello",
                capabilities={
                    "aws_region": "us-east-1",
                    "batch_queue": "nonexistent-queue",
                    "batch_job_def": "test-job-def"
                }
            )
            
            with pytest.raises(Exception) as exc_info:
                future.result(timeout=60)
            
            assert "queue" in str(exc_info.value).lower()
    
    def test_job_timeout(self):
        """Test job timeout handling."""
        with Client(address="tcp://127.0.0.1:2345") as client:
            future = client.submit_command(
                "sleep 300",  # 5 minute sleep
                capabilities={
                    "aws_region": "us-east-1",
                    "batch_queue": "test-queue",
                    "batch_job_def": "test-job-def",
                    "timeout": 60  # 1 minute timeout
                }
            )
            
            result = future.result(timeout=120)
            assert result["status"] in ["TIMEOUT", "FAILED"]
    
    def test_network_failure_recovery(self):
        """Test recovery from network failures."""
        # This would require network simulation tools
        # or mocking of AWS API calls to simulate failures
        pass
```

**Test Cases:**
- [ ] Invalid job queue/definition
- [ ] AWS service unavailability
- [ ] Network connectivity issues
- [ ] Job timeout scenarios
- [ ] Resource limit exceeded
- [ ] Permission denied errors
- [ ] Job cancellation edge cases

## Test Automation

### Continuous Integration Pipeline

#### GitHub Actions Workflow
**File**: `.github/workflows/aws-batch-tests.yml`

```yaml
name: AWS Batch Integration Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    - name: Run unit tests
      run: |
        pytest tests/unit/aws_batch/ -v --cov=scaler.worker_adapter.aws_batch
    
  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    if: github.event_name == 'push'
    steps:
    - uses: actions/checkout@v3
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v1
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-1
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.9'
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    - name: Run integration tests
      run: |
        pytest tests/integration/aws_batch/ -v --timeout=600
```

### Test Data Management

#### Test Resource Cleanup
**File**: `tests/conftest.py`

```python
@pytest.fixture(scope="session", autouse=True)
def cleanup_test_resources():
    """Clean up AWS Batch test resources after test session."""
    yield
    
    # Cleanup logic
    batch_client = boto3.client('batch', region_name='us-east-1')
    
    # Cancel any running test jobs
    jobs = batch_client.list_jobs(
        jobQueue='scaler-test-queue',
        jobStatus='RUNNING'
    )
    
    for job in jobs.get('jobList', []):
        if job['jobName'].startswith('scaler-test-'):
            batch_client.cancel_job(
                jobId=job['jobId'],
                reason='Test cleanup'
            )
```

## Test Execution Plan

### Pre-Release Testing Checklist

#### Phase 1: Unit Tests (Daily)
- [ ] All unit tests pass
- [ ] Code coverage > 80%
- [ ] No new linting errors
- [ ] Documentation updated

#### Phase 2: Integration Tests (Weekly)
- [ ] End-to-end scenarios pass
- [ ] AWS Batch service integration works
- [ ] Multi-region functionality verified
- [ ] Error scenarios handled correctly

#### Phase 3: Performance Tests (Before Release)
- [ ] Load tests pass performance criteria
- [ ] Memory usage within limits
- [ ] Latency requirements met
- [ ] Scalability targets achieved

#### Phase 4: Manual Testing (Before Release)
- [ ] User acceptance scenarios
- [ ] Documentation walkthrough
- [ ] Deployment procedure verified
- [ ] Rollback procedure tested

### Test Environment Requirements

#### AWS Resources
- Test AWS account with Batch permissions
- Compute environments in multiple regions
- Job queues with appropriate capacity
- Job definitions for different scenarios
- CloudWatch logs access

#### Infrastructure
- CI/CD pipeline with AWS access
- Test data management system
- Performance monitoring tools
- Test result reporting dashboard

## Success Criteria

### Functional Requirements
- [ ] All unit tests pass (100%)
- [ ] Integration tests pass (100%)
- [ ] End-to-end scenarios work correctly
- [ ] Error handling meets requirements

### Performance Requirements
- [ ] Job submission latency < 2 seconds
- [ ] Concurrent job handling (50+ jobs)
- [ ] Memory usage < 500MB growth under load
- [ ] Multi-region support with reasonable latency

### Quality Requirements
- [ ] Code coverage > 80%
- [ ] No critical security vulnerabilities
- [ ] Documentation complete and accurate
- [ ] Performance benchmarks established