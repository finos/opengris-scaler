
## Scaler Examples

If you wish to run examples in current working directory, prefix `python run_xxx.py` with `PYTHONPATH=..`

Ensure that the scheduler and cluster are set up before running clients.

- `disconnect_client.py`
    Shows how to disconnect a client from scheduler
- `graphtask_client.py`
    Shows how to send a graph based task to scheduler
- `graphtask_nested_client.py`
    Shows how to dynamically build graph in the remote end
- `map_client.py`
    Shows how to use client.map
- `nested_client.py`
    Shows how to send a nested task to scheduler
- `simple_client.py`
    Shows how to send a basic task to scheduler
- `submit_tasks.py`
    Shows various ways to submit tasks (submit, map, starmap)
- `task_capabilities.py`
    Shows how to use capabilities to route task to various workers
- `ray_compat/`
    Shows how to use Scaler's Ray compatibility layer
