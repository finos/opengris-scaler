import faulthandler
import logging
import sys
import typing

from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.utility.logging.utility import LoggingLevel, LogType, detect_log_type, get_logger_info, setup_logger

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

    # Read the primary logging location back off the logger itself, rather than off log_paths:
    # setup_logger ignores log_paths in favor of logging_config_file when one is given, so log_paths
    # is not a reliable source of truth for where logging actually ended up.
    _, _, resolved_log_paths = get_logger_info(logging.getLogger("scaler"))
    __enable_faulthandler(resolved_log_paths[0])


def __enable_faulthandler(log_path: str) -> None:
    global _faulthandler_file

    if detect_log_type(log_path) == LogType.Screen:
        faulthandler.enable(file=sys.stdout, all_threads=True)
        logger.info("fatal signal crash dumps will be written to stdout")
        return

    try:
        _faulthandler_file = open(log_path, "a")
        faulthandler.enable(file=_faulthandler_file, all_threads=True)
        logger.info(f"fatal signal crash dumps will be written to {log_path!r}")
    except OSError:
        faulthandler.enable(all_threads=True)
        logger.info(f"fatal signal crash dumps will be written to stderr (failed to open {log_path!r})")
