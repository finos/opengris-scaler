import dataclasses
import enum
import importlib
import inspect
import pathlib
import pkgutil
import sys
from typing import Any, Dict, Tuple, Type, Union, get_args, get_origin

import scaler.config.section
from scaler.config.mixins import ConfigType


def find_project_root(marker: str = "pyproject.toml") -> pathlib.Path:
    """Searches upwards from the current script for a project root marker."""
    current_dir = pathlib.Path(__file__).parent.resolve()
    while current_dir != current_dir.parent:
        if (current_dir / marker).exists():
            return current_dir
        current_dir = current_dir.parent
    raise FileNotFoundError(f"Could not find the project root marker '{marker}'")


try:
    SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
    PROJECT_ROOT = find_project_root()
    SCALER_ROOT = PROJECT_ROOT / "scaler"
except FileNotFoundError as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def get_config_classes() -> Dict[str, Type]:
    """Dynamically finds all configuration section classes using pkgutil."""
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    config_classes = {}

    for module_info in pkgutil.iter_modules(scaler.config.section.__path__, f"{scaler.config.section.__name__}."):
        try:
            module = importlib.import_module(module_info.name)
            for name, obj in inspect.getmembers(module, inspect.isclass):

                if obj.__module__ == module_info.name and hasattr(obj, "_is_config_section") and obj._is_config_section:
                    # We still check if it's a dataclass, because the rest of
                    # the script depends on dataclasses.fields()
                    if not dataclasses.is_dataclass(obj):
                        print(
                            f"Warning: Config section {obj} is marked with @config_section "
                            "but is not a dataclass. Skipping."
                        )
                        continue

                    section_name = module_info.name.split(".")[-1]
                    config_classes[section_name] = obj

        except ImportError as e:
            print(f"Warning: Could not import module {module_info.name}. Skipping. Error: {e}")
    return config_classes


def get_type_repr(type_hint: Any) -> str:
    """Creates a user-friendly string representation of a type hint."""
    if inspect.isclass(type_hint):
        if issubclass(type_hint, ConfigType):
            return "str"
        if issubclass(type_hint, enum.Enum):
            choices = ", ".join(e.name for e in type_hint)
            return f"str (choices: {choices})"

    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin is Union and len(args) == 2 and args[1] is type(None):
        return f"Optional[{get_type_repr(args[0])}]"

    if origin in (tuple, Tuple) and len(args) == 2 and args[1] is Ellipsis:
        return f"Tuple[{get_type_repr(args[0])}, ...]"

    if origin:
        origin_name = getattr(origin, "__name__", str(origin)).capitalize()
        args_repr = ", ".join(get_type_repr(arg) for arg in args)
        return f"{origin_name}[{args_repr}]"

    return getattr(type_hint, "__name__", str(type_hint))


def format_value(value: Any) -> str:
    """Formats Python values into valid TOML strings."""
    if isinstance(value, (ConfigType, str)):
        return f'"{value}"'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, enum.Enum):
        return f'"{value.name}"'
    if value is None:
        return '""'
    if isinstance(value, (list, tuple)):
        return f"[{', '.join(format_value(v) for v in value)}]"
    if dataclasses.is_dataclass(value):
        fields = [f"{f.name} = {format_value(getattr(value, f.name))}" for f in dataclasses.fields(value)]
        return f"{{ {', '.join(fields)} }}"
    return str(value)


def generate_toml_string(config_classes: Dict[str, Type]) -> str:
    """Generates the full TOML configuration string."""
    toml_lines = [
        "# Default configuration for OpenGRIS Scaler",
        "# This file is auto-generated. Do not edit manually.",
        "# For required fields, a value must be provided.",
        "",
    ]

    for section_name, config_class in sorted(config_classes.items()):
        toml_lines.append(f"[{section_name}]")
        for field in dataclasses.fields(config_class):
            type_repr = get_type_repr(field.type)

            if field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING:
                toml_lines.append(f"# Type: {type_repr} (REQUIRED)")
                toml_lines.append(f'{field.name} = ""')
                continue

            toml_lines.append(f"# Type: {type_repr}")

            default_value = field.default
            if field.default_factory is not dataclasses.MISSING:
                try:
                    default_value = field.default_factory()
                except Exception as e:
                    print(f"Warning: Could not execute default_factory for {field.name}. Error: {e}")
                    continue

            formatted_default = format_value(default_value)
            toml_lines.append(f"{field.name} = {formatted_default}")
        toml_lines.append("")

    return "\n".join(toml_lines)


def main():
    classes = get_config_classes()
    if not classes:
        print("Error: No configuration classes found.", file=sys.stderr)
        sys.exit(1)

    toml_content = generate_toml_string(classes)

    output_dir = SCRIPT_DIR.parent / "_static"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "example_config.toml"

    with open(output_path, "w") as f:
        f.write(toml_content)

    print(f"Successfully generated docs TOML config at: {output_path}")


if __name__ == "__main__":
    main()
