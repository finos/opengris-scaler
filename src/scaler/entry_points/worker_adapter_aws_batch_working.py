#!/usr/bin/env python3
"""
AWS Batch Worker Adapter with real workers that execute tasks.
"""

import argparse
import asyncio
import logging
import multiprocessing
from aiohttp import web

# Import real Scaler worker to execute tasks
from scaler.worker.worker import Worker
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.identifiers import WorkerID


class WorkingAWSBatchWorker:
    def __init__(self, name, scheduler_address):
        self.name = name
        self.identity = WorkerID.generate_worker_id(name)
        self._scheduler_address = ZMQConfig.from_string(scheduler_address)
        self._worker_process = None
    
    def start(self):
        print(f"Starting AWS Batch worker {self.name}")
        # Create a real Scaler worker process
        self._worker_process = Worker(
            event_loop="builtin",
            name=self.name,
            address=self._scheduler_address,
            object_storage_address=None,
            preload=None,
            capabilities={},
            io_threads=1,
            task_queue_size=1000,
            heartbeat_interval_seconds=2,
            garbage_collect_interval_seconds=60,
            trim_memory_threshold_bytes=1024*1024*100,
            task_timeout_seconds=300,
            death_timeout_seconds=60,
            hard_processor_suspend=False,
            logging_paths=("/dev/stdout",),
            logging_level="INFO"
        )
        self._worker_process.start()
        print(f"AWS Batch worker {self.name} started with PID {self._worker_process.pid}")
    
    def terminate(self):
        print(f"Terminating AWS Batch worker {self.name}")
        if self._worker_process and self._worker_process.is_alive():
            self._worker_process.terminate()
            self._worker_process.join(timeout=5)
        print(f"AWS Batch worker {self.name} terminated")


class WorkingAWSBatchWorkerAdapter:
    def __init__(self, scheduler_address):
        self._scheduler_address = scheduler_address
        self._worker_groups = {}
    
    async def start_worker_group(self):
        worker_group_id = f"aws-batch-group-1".encode()
        worker = WorkingAWSBatchWorker("aws-batch-worker-1", self._scheduler_address)
        worker.start()
        self._worker_groups[worker_group_id] = {worker.identity: worker}
        return worker_group_id
    
    async def shutdown_worker_group(self, worker_group_id):
        if worker_group_id in self._worker_groups:
            for worker in self._worker_groups[worker_group_id].values():
                worker.terminate()
            del self._worker_groups[worker_group_id]
    
    async def webhook_handler(self, request):
        try:
            request_json = await request.json()
        except:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        action = request_json.get("action")
        
        if action == "get_worker_adapter_info":
            return web.json_response({
                "max_worker_groups": 1,
                "workers_per_group": 1,
                "base_capabilities": {},
                "adapter_type": "aws_batch_working"
            })
        elif action == "start_worker_group":
            worker_group_id = await self.start_worker_group()
            return web.json_response({
                "status": "Worker group started",
                "worker_group_id": worker_group_id.decode(),
                "worker_ids": [str(wid) for wid in self._worker_groups[worker_group_id].keys()]
            })
        elif action == "shutdown_worker_group":
            worker_group_id = request_json.get("worker_group_id", "").encode()
            await self.shutdown_worker_group(worker_group_id)
            return web.json_response({"status": "Worker group shutdown"})
        else:
            return web.json_response({"error": "Unknown action"}, status=400)
    
    def create_app(self):
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app


def main():
    parser = argparse.ArgumentParser(description="Working AWS Batch Worker Adapter")
    parser.add_argument("scheduler_address", help="Scheduler address (e.g., tcp://127.0.0.1:2345)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind (default: 8080)")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    adapter = WorkingAWSBatchWorkerAdapter(args.scheduler_address)
    app = adapter.create_app()
    
    print(f"Starting working AWS Batch worker adapter on {args.host}:{args.port}")
    print(f"Scheduler: {args.scheduler_address}")
    
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()