"""Worker context management for nested client functionality."""

import os
import threading
from typing import Optional
from scaler.utility.zmq_config import ZMQConfig

# Thread-local storage to track worker scheduler addresses
_worker_context = threading.local()


def set_worker_scheduler_address(address: ZMQConfig) -> None:
    """
    Set the scheduler address for the current worker context.
    
    This should be called by the worker to register its scheduler address
    so that nested clients can automatically use it.
    
    Args:
        address: The ZMQ address the worker uses to connect to the scheduler
    """
    _worker_context.scheduler_address = address


def get_worker_scheduler_address() -> Optional[ZMQConfig]:
    """
    Get the scheduler address for the current worker context.
    
    This checks:
    1. Thread-local worker context (for worker processes)
    2. Environment variable (for child processes like processors)
    
    Returns:
        The scheduler address if running in a worker context, None otherwise
    """
    # First check thread-local storage
    address = getattr(_worker_context, 'scheduler_address', None)
    if address is not None:
        return address
    
    # Check environment variable (for child processes)
    env_address = os.environ.get('SCALER_WORKER_SCHEDULER_ADDRESS')
    if env_address:
        try:
            return ZMQConfig.from_string(env_address)
        except Exception:
            # Invalid address format, ignore
            pass
    
    return None


def clear_worker_scheduler_address() -> None:
    """Clear the worker scheduler address from the current context."""
    if hasattr(_worker_context, 'scheduler_address'):
        delattr(_worker_context, 'scheduler_address')


def is_running_in_worker() -> bool:
    """
    Check if the current code is running inside a worker context.
    
    This can detect both:
    1. Running in a worker process (through processor context)
    2. Running in a context with registered scheduler address
    
    Returns:
        True if running in a worker context, False otherwise
    """
    # Try to import and check processor context
    try:
        from scaler.worker.agent.processor.processor import Processor
        if Processor.get_current_processor() is not None:
            return True
    except ImportError:
        pass
    
    # Check if worker scheduler address is available
    return get_worker_scheduler_address() is not None