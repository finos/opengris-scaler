#!/usr/bin/env python3
"""
Single-process test for debugging on_new_task() method.
Runs scheduler, adapter, and client all in the same process.
"""

import asyncio
import math
import sys
import os
from pathlib import Path

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from scaler import Client, SchedulerClusterCombo

async def main():
    print("Starting single-process test...")
    
    # Start scheduler and worker in same process
    cluster = SchedulerClusterCombo(
        address="tcp://127.0.0.1:2345", 
        n_workers=1
    )
    
    try:
        print("Waiting for cluster to start...")
        await asyncio.sleep(2)
        
        print("\n" + "="*50)
        print("SUBMITTING TEST TASK - on_new_task() SHOULD BE CALLED NOW")
        print("="*50)
        
        # Submit task - this will trigger on_new_task() in same process
        with Client(address="tcp://127.0.0.1:2345") as client:
            print('Submitting task...')
            future = client.submit(math.sqrt, 16)
            print('Task submitted, waiting for result...')
            result = future.result()
            print(f'Result: {result}')
        
        print("\n✓ Test completed successfully!")
        
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    asyncio.run(main())