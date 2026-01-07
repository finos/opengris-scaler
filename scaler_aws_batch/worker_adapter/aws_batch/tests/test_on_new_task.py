#!/usr/bin/env python3
"""
Test harness for debugging on_new_task() implementation.
Starts scheduler, adapter, and submits a single task to trigger on_new_task().
"""

import os
import sys
import time
import signal
import subprocess
import requests
from pathlib import Path

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
os.chdir(PROJECT_ROOT)

# Set PYTHONPATH
os.environ['PYTHONPATH'] = 'src'

def run_process(cmd, name):
    """Start a subprocess and return the process object."""
    print(f"Starting {name}...")
    process = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
    return process

def cleanup_processes(processes):
    """Clean up all processes."""
    print("\nCleaning up processes...")
    for name, process in processes.items():
        if process and process.poll() is None:
            print(f"Terminating {name}...")
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=5)
            except:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except:
                    pass

def main():
    processes = {}
    
    try:
        # 1. Start Scheduler
        scheduler_cmd = f"python {PROJECT_ROOT}/src/run_scheduler.py tcp://127.0.0.1:2345"
        processes['scheduler'] = run_process(scheduler_cmd, "Scheduler")
        
        print("Waiting for scheduler to start...")
        time.sleep(3)
        
        # 2. Start AWS Batch Worker Adapter
        adapter_cmd = f"python {PROJECT_ROOT}/src/scaler/entry_points/worker_adapter_aws_batch_with_task_manager.py tcp://127.0.0.1:2345 --port 8080"
        processes['adapter'] = run_process(adapter_cmd, "AWS Batch Worker Adapter")
        
        print("Waiting for adapter to start...")
        time.sleep(3)
        
        # 3. Start Worker Group
        print("Starting worker group...")
        response = requests.post(
            "http://127.0.0.1:8080",
            json={"action": "start_worker_group"},
            timeout=10
        )
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Worker group started: {result['worker_group_id']}")
        else:
            print(f"ERROR: Failed to start worker group: {response.text}")
            return 1
        
        print("Waiting for worker to connect...")
        time.sleep(3)
        
        # 4. Submit a single task to trigger on_new_task()
        print("\n" + "="*50)
        print("SUBMITTING TEST TASK - on_new_task() SHOULD BE CALLED NOW")
        print("="*50)
        
        test_cmd = f"""python -c "
from scaler import Client
import math

with Client(address='tcp://127.0.0.1:2345') as client:
    print('Submitting task...')
    future = client.submit(math.sqrt, 16)
    print('Task submitted, waiting for result...')
    result = future.result()
    print(f'Result: {{result}}')
"
"""
        
        print("Executing test task...")
        test_process = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)
        
        print("STDOUT:")
        print(test_process.stdout)
        if test_process.stderr:
            print("STDERR:")
            print(test_process.stderr)
        
        if test_process.returncode == 0:
            print("\n✓ Test task completed successfully!")
            return 0
        else:
            print(f"\n✗ Test task failed with return code {test_process.returncode}")
            return 1
            
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        return 1
    except Exception as e:
        print(f"\nERROR: {e}")
        return 1
    finally:
        cleanup_processes(processes)

if __name__ == "__main__":
    sys.exit(main())