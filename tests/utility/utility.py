import logging
import multiprocessing
import unittest

# Shared timing bounds for tests that spawn real processes / clusters: one poll granularity and one
# "wait for a spawned process to terminate" bound, so the process-lifecycle tests do not each
# hard-code their own. Only reached in full on failure, so the bound errs on the side of patience.
POLL_INTERVAL_SECONDS = 0.05
PROCESS_TERMINATION_TIMEOUT_SECONDS = 30


def terminate_process(process: multiprocessing.process.BaseProcess) -> None:
    """Reap a spawned test process: terminate it gracefully, then force-kill if it does not exit."""
    if process.is_alive():
        process.terminate()
        process.join(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
    if process.is_alive():
        process.kill()
        process.join()


# Global variable to test preload functionality
PRELOAD_VALUE = None


def logging_test_name(obj: unittest.TestCase):
    logging.info(f"{obj.__class__.__name__}:{obj._testMethodName} ==============================================")


def setup_global_value(value: str = "default") -> None:
    """Preload function that sets a global variable"""
    global PRELOAD_VALUE
    PRELOAD_VALUE = value
    logging.info(f"Preload set PRELOAD_VALUE to: {value}")


def get_global_value():
    """Function to be called by tasks to retrieve the preloaded value"""
    return PRELOAD_VALUE


def failing_preload():
    """Preload function that always fails"""
    raise ValueError("Intentional preload failure for testing")
