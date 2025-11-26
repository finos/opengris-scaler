#!/usr/bin/env python3
"""
Single script to run complete AWS Batch worker adapter test.
Starts all prerequisites and runs the test.
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

def wait_for_port(port, timeout=30):
    """Wait for a port to be available."""
    import socket
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', port))
            sock.close()
            if result == 0:
                return True
        except:
            pass
        time.sleep(0.5)
    return False

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
        scheduler_cmd = "python ./src/run_scheduler.py tcp://127.0.0.1:2345"
        processes['scheduler'] = run_process(scheduler_cmd, "Scheduler")
        
        # Wait for scheduler to start
        print("Waiting for scheduler to start...")
        time.sleep(3)
        
        # 2. Start AWS Batch Worker Adapter
        adapter_cmd = "python src/scaler/entry_points/worker_adapter_aws_batch_working.py tcp://127.0.0.1:2345 --port 8080"
        processes['adapter'] = run_process(adapter_cmd, "AWS Batch Worker Adapter")
        
        # Wait for adapter to start
        print("Waiting for adapter to start...")
        if not wait_for_port(8080):
            print("ERROR: Adapter failed to start on port 8080")
            return 1
        
        time.sleep(2)
        
        # 3. Start Worker Group via API
        print("Starting worker group...")
        try:
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
        except Exception as e:
            print(f"ERROR: Failed to start worker group: {e}")
            return 1
        
        # Wait for worker to connect to scheduler
        print("Waiting for worker to connect...")
        time.sleep(3)
        
        # 4. Run Test Harness
        print("\n" + "="*50)
        print("RUNNING AWS BATCH TEST")
        print("="*50)
        
        test_cmd = "python scaler_aws_batch/worker_adapter/aws_batch/tests/aws_batch_test_harness.py --scheduler tcp://127.0.0.1:2345"
        test_process = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)
        
        print("STDOUT:")
        print(test_process.stdout)
        if test_process.stderr:
            print("STDERR:")
            print(test_process.stderr)
        
        if test_process.returncode == 0:
            print("\n✓ AWS Batch test completed successfully!")
            return 0
        else:
            print(f"\n✗ AWS Batch test failed with return code {test_process.returncode}")
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