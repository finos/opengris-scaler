import functools
import pdb
import sys
from typing import Any, Callable, Optional


def pdb_wrapped(func: Callable[..., Optional[int]]):
    @functools.wraps(func)
    def pdb_wrapper(*args: Any, **kwargs: Any):
        try:
            exit_code = func(*args, **kwargs)
            sys.exit(exit_code)

        except Exception:
            ex_type, value, tb = sys.exc_info()
            pdb.post_mortem(tb)
            raise

    return pdb_wrapper
