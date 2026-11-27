import logging
import unittest
from typing import NoReturn, Optional

# Global variable to test preload functionality
PRELOAD_VALUE: Optional[str] = None


def logging_test_name(obj: unittest.TestCase) -> None:
    logging.info(f"{obj.__class__.__name__}:{obj._testMethodName} ==============================================")


def setup_global_value(value: str = "default") -> None:
    """Preload function that sets a global variable"""
    global PRELOAD_VALUE
    PRELOAD_VALUE = value
    logging.info(f"Preload set PRELOAD_VALUE to: {value}")


def get_global_value() -> Optional[str]:
    """Function to be called by tasks to retrieve the preloaded value"""
    return PRELOAD_VALUE


def failing_preload() -> NoReturn:
    """Preload function that always fails"""
    raise ValueError("Intentional preload failure for testing")
