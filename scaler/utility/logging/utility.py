import dataclasses
import enum
import logging
import logging.config
import os
import typing


class LogType(enum.Enum):
    Screen = enum.auto()
    File = enum.auto()


@dataclasses.dataclass
class LogPath:
    log_type: LogType
    path: str


class LoggingLevel(enum.Enum):
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


def setup_logger(
    log_paths: typing.Tuple[str, ...] = ("/dev/stdout",),
    logging_config_file: typing.Optional[str] = None,
    logging_level: str = LoggingLevel.INFO.name,
):
    if not log_paths and not logging_config_file:
        return

    if logging_config_file is not None:
        print(f"use logging config file: {logging_config_file}")
        logging.config.fileConfig(logging_config_file, disable_existing_loggers=True)
        return

    resolved_log_paths = [LogPath(log_type=__detect_log_types(file_name), path=file_name) for file_name in log_paths]
    __logging_config(log_paths=resolved_log_paths, logging_level=logging_level)
    logging.info(f"logging to {log_paths}")


def __detect_log_types(file_name: str) -> LogType:
    if file_name in {"-", "/dev/stdout"}:
        return LogType.Screen

    return LogType.File


def __format(name) -> str:
    if not name:
        return ""

    return "%({name})s".format(name=name)


def __generate_log_config() -> typing.Dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,  # this fixes the problem
        "formatters": {
            "standard": {
                "format": "[{levelname}]{asctime}: {message}".format(
                    levelname=__format("levelname"), asctime=__format("asctime"), message=__format("message")
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
            },
            "verbose": {
                "format": "[{levelname}]{asctime}:{module}:{funcName}:{lineno}: {message}".format(
                    levelname=__format("levelname"),
                    asctime=__format("asctime"),
                    module=__format("module"),
                    funcName=__format("funcName"),
                    lineno=__format("lineno"),
                    message=__format("message"),
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S%z",
            },
        },
        "handlers": {},
        "loggers": {"": {"handlers": [], "level": "DEBUG", "propagate": True}},
    }


def __logging_config(log_paths: typing.List[LogPath], logging_level: str = LoggingLevel.INFO.name):
    logging.addLevelName(logging.INFO, "INFO")
    logging.addLevelName(logging.WARNING, "WARN")
    logging.addLevelName(logging.ERROR, "EROR")
    logging.addLevelName(logging.DEBUG, "DEBG")
    logging.addLevelName(logging.CRITICAL, "CTIC")

    config = __generate_log_config()
    handlers = config["handlers"]
    root_loggers = config["loggers"][""]["handlers"]

    for log_path in log_paths:
        if log_path.log_type == LogType.Screen:
            handlers["console"] = __create_stdout_handler(logging_level)
            root_loggers.append("console")
            continue

        elif log_path.log_type == LogType.File:
            handlers[log_path.path] = __create_time_rotating_file_handler(logging_level, log_path.path)
            root_loggers.append(log_path.path)
            continue

        raise TypeError(f"Unsupported LogPath: {log_path}")

    logging.config.dictConfig(config)


def __create_stdout_handler(logging_level: str):
    return {
        "class": "logging.StreamHandler",
        "level": logging_level,
        "formatter": "standard",
        "stream": "ext://sys.stdout",
    }


def __create_time_rotating_file_handler(logging_level: str, file_path: str):
    return {
        "class": "logging.handlers.TimedRotatingFileHandler",
        "level": logging_level,
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(file_path)),
        "when": "midnight",
    }


def __create_size_rotating_file_handler(log_path) -> typing.Dict:
    return {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "INFO",
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(log_path)),
        "maxBytes": 10485760,
        "backupCount": 20,
        "encoding": "utf8",
    }


def __parse_logging_level(value):
    return LoggingLevel(value).value
