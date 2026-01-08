#!/usr/bin/env python3
"""
AWS Batch Worker Adapter that uses the actual AWS Batch task manager.
"""

import argparse
import asyncio
import logging
from aiohttp import web

from scaler.worker_adapter.aws_batch.worker import AWSBatchWorker
from scaler.config.types.zmq import ZMQConfig


class TestAWSBatchWorkerAdapter:
    def __init__(self, scheduler_address):
        self._scheduler_address = scheduler_address
        self._workers = {}
    
    async def start_worker_group(self):
        worker_id = "aws-batch-worker-1"
        worker = AWSBatchWorker(
            name=worker_id,
            address=ZMQConfig.from_string(self._scheduler_address),
            object_storage_address=None,
            job_queue="test-queue",
            job_definition="test-job-def",
            aws_region="us-east-1",
            capabilities={},
            base_concurrency=1,
            heartbeat_interval_seconds=2,
            death_timeout_seconds=60,
            task_queue_size=1000,
            io_threads=1,
            event_loop="builtin",
            vcpus=1,
            memory=2048
        )
        worker.start()
        self._workers[worker_id] = worker
        return worker_id.encode()
    
    async def shutdown_worker_group(self, worker_group_id):
        worker_id = worker_group_id.decode()
        if worker_id in self._workers:
            self._workers[worker_id].terminate()
            del self._workers[worker_id]
    
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
                "adapter_type": "aws_batch_with_task_manager"
            })
        elif action == "start_worker_group":
            worker_group_id = await self.start_worker_group()
            return web.json_response({
                "status": "Worker group started",
                "worker_group_id": worker_group_id.decode(),
                "worker_ids": [worker_group_id.decode()]
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
    parser = argparse.ArgumentParser(description="AWS Batch Worker Adapter with Task Manager")
    parser.add_argument("scheduler_address", help="Scheduler address")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    adapter = TestAWSBatchWorkerAdapter(args.scheduler_address)
    app = adapter.create_app()
    
    print(f"Starting AWS Batch adapter with task manager on {args.host}:{args.port}")
    
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()