import dataclasses
from abc import ABC, abstractmethod
from typing import Type, TypeVar

from configargparse import ArgParser, ArgumentDefaultsHelpFormatter, TomlConfigParser

T = TypeVar("T", bound="ConfigClass")


class ConfigClass(ABC):
    """
    A dataclass where the fields define command line options, config file options, and environment variables.

    Subclasses of `ConfigClass` must be dataclasses.

    The name of the data class field (replacing underscores with hyphens) are used
    as the long option name for command-line parameters.
    A short name for the argument can be provided in the field metadata using the "short" key.
    The value is the short option name and must include the hyphen, e.g. "-n".
    You can also override the long name using the "long" key, and the name must includes the
    hyphens as well, e.g. "--number".
    The default value of the field is also used as the default for the argument parser.
    Any additional fields present in the metadata dict are passed through to `add_argument()`
    and take precedence over other sources.

    There is one restriction: `--config` and `-c` are reserved for the config file.
    """

    @staticmethod
    @abstractmethod
    def section_name() -> str:
        """the section name for this config in a toml file"""
        ...

    @staticmethod
    @abstractmethod
    def description() -> str:
        """the description of this entrypoint used in the help"""
        ...

    @classmethod
    def parse(cls: Type[T]) -> T:
        if not dataclasses.is_dataclass(cls):
            raise RuntimeError("config class must be a dataclass")

        parser = ArgParser(
            cls.description(),
            formatter_class=ArgumentDefaultsHelpFormatter,
            config_file_parser_class=TomlConfigParser(sections=[cls.section_name()]),
        )

        parser.add_argument("--config", "-c", is_config_file=True, help="Path to the TOML configuration file.")

        for field in dataclasses.fields(cls):
            kwargs = dict(field.metadata)

            long_name = kwargs.pop("long", f"--{field.name.replace('_', '-')}")
            if "short" in field.metadata:
                args = [long_name, kwargs.pop("short")]
            else:
                args = [long_name]

            if field.default != dataclasses.MISSING:
                kwargs["default"] = field.default

            if field.default_factory != dataclasses.MISSING:
                kwargs["default"] = field.default_factory()

            # when store true or store false is set, setting the type raises a type error
            if kwargs.get("action") not in ("store_true", "store_false"):
                kwargs["type"] = field.type

            parser.add_argument(*args, **kwargs)

        args = vars(parser.parse_args())

        # drop the config argument, the config class isn't aware of it
        args.pop("config")

        return cls(**args)
