import signal
from typing import Optional


def describe_exitcode(exitcode: Optional[int]) -> str:
    """Render a process exit code, naming the signal for signal-terminated processes.

    multiprocessing reports a process killed by signal N as exit code -N, so an OOM kill (SIGKILL) shows
    as -9. Turn that into e.g. "-9 (SIGKILL)" so the cause is legible in logs.
    """
    if exitcode is not None and exitcode < 0:
        try:
            return f"{exitcode} ({signal.Signals(-exitcode).name})"
        except ValueError:
            pass
    return str(exitcode)
