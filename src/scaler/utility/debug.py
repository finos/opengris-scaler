import functools
import pdb
import sys
from typing import Callable


def pdb_wrapped(func: Callable):
    @functools.wraps(func)
    def pdb_wrapper(*args, **kwargs):
        try:
            exit_code = func(*args, **kwargs)
            sys.exit(exit_code)

        except Exception:
            ex_type, value, tb = sys.exc_info()
            pdb.post_mortem(tb)
            raise

    return pdb_wrapper
