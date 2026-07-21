import faulthandler
import logging
import os
import typing

from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.utility.logging.utility import LoggingLevel, setup_logger

logger = logging.getLogger(__name__)

_faulthandler_file: typing.Optional[typing.IO] = None


def bootstrap_process(
    log_paths: typing.Tuple[str, ...] = DEFAULT_LOGGING_PATHS,
    logging_config_file: typing.Optional[str] = None,
    logging_level: str = LoggingLevel.INFO.name,
    process_name: str = "scaler",
) -> None:
    """Configure logging and enable a fatal-signal crash-dump handler for this process."""
    setup_logger(log_paths, logging_config_file, logging_level, process_name)
    __enable_faulthandler(log_paths[0] if log_paths else None)


def __enable_faulthandler(log_path: typing.Optional[str]) -> None:
    global _faulthandler_file

    if log_path is None:
        faulthandler.enable(all_threads=True)
        logger.info("fatal signal crash dumps will be written to stderr")
        return

    try:
        _faulthandler_file = open(os.path.expandvars(os.path.expanduser(log_path)), "a")
        faulthandler.enable(file=_faulthandler_file, all_threads=True)
        logger.info(f"fatal signal crash dumps will be written to {log_path!r}")
    except OSError:
        faulthandler.enable(all_threads=True)
        logger.info(f"fatal signal crash dumps will be written to stderr (failed to open {log_path!r})")
