import logging

# Global variable to test preload functionality
PRELOAD_VALUE = None


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
