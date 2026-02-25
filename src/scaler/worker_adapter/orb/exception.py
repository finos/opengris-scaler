from typing import Any


class ORBException(Exception):
    """Exception raised for errors in ORB operations."""

    def __init__(self, data: Any):
        self.data = data
        super().__init__(f"ORB Exception: {data}")
