#!/usr/bin/env python3
"""
Minimal AWS Batch Worker Adapter for testing on_task_new().
"""
import argparse
import asyncio
import logging
from aiohttp import web

# Mock AWS Batch components for testing
class MockAWSBatchWorker:
    def __init__(self, name):
        self.name = name
        self.identity = f"aws-batch-{name}"
    
    def start(self):
        print(f"Mock AWS Batch worker {self.name} started")
    
    def terminate(self):
        print(f"Mock AWS Batch worker {self.name} terminated")


class MockAWSBatchWorkerAdapter:
    def __init__(self):
        self._worker_groups = {}
    
    async def start_worker_group(self):
        worker_group_id = f"aws-batch-group-1".encode()
        worker = MockAWSBatchWorker("test-worker")
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
                "adapter_type": "aws_batch_mock"
            })
        elif action == "start_worker_group":
            worker_group_id = await self.start_worker_group()
            return web.json_response({
                "status": "Worker group started",
                "worker_group_id": worker_group_id.decode(),
                "worker_ids": list(self._worker_groups[worker_group_id].keys())
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
    parser = argparse.ArgumentParser(description="Minimal AWS Batch Worker Adapter")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind (default: 8080)")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    adapter = MockAWSBatchWorkerAdapter()
    app = adapter.create_app()
    
    print(f"Starting minimal AWS Batch worker adapter on {args.host}:{args.port}")
    print("This is a mock adapter for testing purposes only")
    
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()