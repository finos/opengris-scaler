import argparse
import dataclasses
import enum
import sys
from typing import Any, cast, Dict, Optional, Type, TypeVar, Union, get_args, get_origin

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from scaler.config.mixins import ConfigType

T = TypeVar("T")


def load_config(
    config_class: Type[T], config_path: Optional[str], args: argparse.Namespace, section_name: Optional[str] = None
) -> T:
    """
    Loads configuration for a given dataclass from a TOML file and overrides it with command-line arguments.
    """
    if not dataclasses.is_dataclass(config_class):
        raise TypeError(f"{config_class.__name__} is not a dataclass and cannot be used with this config loader.")

    config_from_file = {}
    if config_path:
        try:
            with open(config_path, "rb") as f:
                try:
                    full_config = tomllib.load(f)
                except tomllib.TOMLDecodeError as e:
                    raise ValueError(f"Error parsing TOML file at {config_path}: {e}") from e

                if section_name:
                    config_from_file = full_config.get(section_name, {})
                else:
                    config_from_file = full_config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    config_from_args = {k: v for k, v in vars(args).items() if v is not None}
    merged_config_data = {**config_from_file, **config_from_args}

    valid_keys = {f.name for f in dataclasses.fields(config_class)}
    unknown_keys = set(merged_config_data.keys()) - valid_keys - {"config"}
    if unknown_keys:
        raise ValueError(f"Unknown configuration key(s) for {config_class.__name__}: {', '.join(unknown_keys)}")

    final_kwargs: Dict[str, Any] = {}
    for field in dataclasses.fields(config_class):
        if field.name in merged_config_data:
            raw_value = merged_config_data[field.name]
            field_type = field.type
            is_optional = get_origin(field_type) is Union
            if is_optional:
                possible_types = [t for t in get_args(field_type) if t is not type(None)]
                actual_type = possible_types[0] if possible_types else field_type
            else:
                actual_type = field_type

            if (
                isinstance(raw_value, str)
                and isinstance(actual_type, type)
                and issubclass(actual_type, ConfigType)
                and not isinstance(raw_value, actual_type)
            ):
                final_kwargs[field.name] = actual_type.from_string(raw_value)
            elif isinstance(raw_value, str) and isinstance(actual_type, type) and issubclass(actual_type, enum.Enum):
                try:
                    final_kwargs[field.name] = actual_type[raw_value]
                except KeyError as e:
                    raise ValueError(f"'{raw_value}' is not a valid member for {actual_type.__name__}") from e
            elif isinstance(raw_value, list) and get_origin(field.type) is tuple:
                final_kwargs[field.name] = tuple(raw_value)
            else:
                final_kwargs[field.name] = raw_value

    try:
        return cast(T, config_class(**final_kwargs))
    except TypeError as e:
        missing_fields = [
            f.name
            for f in dataclasses.fields(config_class)
            if f.init
            and f.name not in final_kwargs
            and f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required configuration arguments: {', '.join(missing_fields)}. "
                f"Please provide them via command line or a TOML config file."
            ) from e
        else:
            raise e
