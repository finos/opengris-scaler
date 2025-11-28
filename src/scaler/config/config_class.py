import dataclasses
import typing
from typing import Any, Dict, Type, TypeVar

from configargparse import ArgParser, ArgumentDefaultsHelpFormatter, TomlConfigParser

from scaler.config.mixins import ConfigType

T = TypeVar("T", bound="ConfigClass")


class ConfigClass:
    """
    An abstract interface for dataclasses where the fields define command line options,
    config file options, and environment variables.

    Subclasses of `ConfigClass` must be dataclasses.
    Subclasses of `ConfigClass` are called "config classes".

    ## Config Files

    All options with a long name can be parsed from a TOML config file specified with `--config` or `-c`.
    The section name is determined by the subclass' implementation of `.section_name()`.

    ## Environment Variables

    Any parameter can be configured to read from an environment variable by adding `env="NAME"` to the field metadata.

    ```python
    # can be set as --my-field on the command line or in a config file, or using the environment variable `NAME`
    my_field: int = dataclasses.field(metadata=dict(env="NAME"))
    ```

    ## Precedence

    When a parameter is supplied in multiple ways, the following precedence is observed:
    command line > environment variables > config file > defaults

    ## Customization

    Any key values included in the field's metadata will be passed through to `.add_argument()`
    and will always take precedence over derived values, except for `default`.

    The value for `default` is taken from the field's default. Providing `default` in the field
    metadata will result in a `TypeError`.

    ```python
    # passes through options
    custom_field: int = dataclasses.field(
        metadata=dict(
            choices=["apples", "oranges"],
            help="choose a fruit",
        )
    )

    # TypeError!
    bad: int = dataclasses.field(metadata=dict(default=0))
    ```

    ## Naming Fields

    The name of the dataclass fields (replacing underscores with hyphens) is used
    as the long option name for command line and config file parameters.
    A short name for the argument can be provided in the field metadata using the `short` key.
    The value is the short option name and must include the hyphen, e.g. `-n`.

    There is one restriction: `--config` and `-c` are reserved for the config file.

    ```python
    # this will have long name --field-one, and no short name
    field_one: int = 5

    # sets a short name
    field_two: int = dataclasses.field(default=5, metadata=dict(short="-f2"))
    ```

    ## Default Values

    The default value of the field is also used as the default for the argument parser.

    ```python
    lang: str = "en"
    name: str = dataclasses.field(default="Arthur")
    ```

    ## Positional Parameters

    You can set `positional=True` in the metadata dict to make an argument positional.
    In this case long and short names are ignored, use `name` to override the name of the option.
    The position is dependent on field ordering.

    ```python
    # both of these are positional, and field one must be specified before field two
    field_one: int = dataclasses.field(metadata=dict(positional=True))
    field_two: int = dataclasses.field(metadata=dict(positional=True))
    ```

    ## Composition

    Config classes can be composed. If a config class has fields that are config classes,
    then the options of the child config class are inherited as if that child config class'
    fields were added to the parent, for the purpose of parsing arguments. When the dataclass
    is created, the structure is kept.

    Care needs to be taken so that field names do not conflict with each other.
    The name of fields in nested config classes are in the same namespace as those
    in the parent and other nested config classes.

    ## Parameter Types

    The type of a field is used as the type in argument parsing, meaning that it must be able
    to be directly constructed from a string.

    Special handling is implemented for several common types:
    - `bool`: parses from "true" and "false", and all upper/lower case variants
    - subclasses of `ConfigType`: uses the `.from_string()` method
    - `Optional[T]`, `T | None`: parsed as `T` and sets `required=False`
    - `List[T]`, `list[T]`: parsed as `T`, and sets `nargs="*"`

    For generic types, the `T` is parsed recursively following the rules here.
    For example, `Optional[T]` where `T` is a subclass of `ConfigType`
    will still use `T.from_string()`.

    as usual, all of these can be overriden by setting the option in the metadata.

    ```python
    # this provides a custom `type` to parse the input as hexadecimal
    # `type` must be a callable that accepts a string
    # refer to the argparse docs for more
    hex: int = dataclasses.field(metadata=dict(type=lambda s: int(s, 16)))

    class MyConfigType(ConfigType):
        ...

    # this will work as expected
    my_field: MyConfigType

    # this requires special handling
    tuples: Tuple[int, ...] = dataclasses.field(metadata=dict(type=int, nargs="*"))

    # works automatically, defaults to `nargs="*"`
    integers: List[int]

    # ... but we can override that
    integers2: List[int] = dataclasses.field(metadata=dict(nargs="+"))

    # this will automatically have `required=False` set
    maybe: Optional[str]
    """

    @classmethod
    def configure_parser(cls: type, parser: ArgParser):
        fields = dataclasses.fields(cls)

        for field in fields:
            if is_config_class(field.type):
                field.type.configure_parser(parser)  # type: ignore[union-attr]
                continue

            kwargs = dict(field.metadata)

            # usually command line options use hyphens instead of underscores

            if kwargs.pop("positional", False):
                args = [kwargs.pop("name", field.name)]
            else:
                long_name = kwargs.pop("long", f"--{field.name.replace('_', '-')}")
                if "short" in kwargs:
                    args = [long_name, kwargs.pop("short")]
                else:
                    args = [long_name]

                # this sets the key given back when args are parsed
                kwargs["dest"] = field.name

            if "default" in kwargs:
                raise TypeError("'default' cannot be provided in field metadata")

            if field.default != dataclasses.MISSING:
                kwargs["default"] = field.default

            if field.default_factory != dataclasses.MISSING:
                kwargs["default"] = field.default_factory()

            # when store true or store false is set, setting the type raises a type error
            if kwargs.get("action") not in ("store_true", "store_false"):

                # sometimes the user will set the type manually
                # this is required for types such as `Option[T]`, where they cannt be directly constructed from a string
                if "type" not in kwargs:
                    opts = get_type_args(field.type)

                    # set all of the options, except where already set
                    for key, value in opts.items():
                        if key not in kwargs:
                            kwargs[key] = value

            parser.add_argument(*args, **kwargs)

    @classmethod
    def parse(cls: Type[T], program_name: str, section: str) -> T:
        parser = ArgParser(
            program_name,
            formatter_class=ArgumentDefaultsHelpFormatter,
            config_file_parser_class=TomlConfigParser(sections=[section]),
        )

        parser.add_argument("--config", "-c", is_config_file=True, help="Path to the TOML configuration file.")
        cls.configure_parser(parser)

        kwargs = vars(parser.parse_args())

        # remove this from the args
        kwargs.pop("config")

        # we need to manually handle any ConfigClass fields
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            if is_config_class(field.type):

                # steal arguments for the config class
                inner_kwargs = {}
                for f in dataclasses.fields(field.type):  # type: ignore[arg-type]
                    if f.name in kwargs:
                        inner_kwargs[f.name] = kwargs.pop(f.name)

                # instantiate and update the args
                kwargs[field.name] = field.type(**inner_kwargs)  # type: ignore[operator]

        return cls(**kwargs)


def parse_bool(s: str) -> bool:
    """parse a bool from a conventional string representation"""

    lower = s.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False

    raise TypeError(f"[{s}] is not a valid bool")


def is_optional(ty: Any) -> bool:
    """determines if `ty` is typing.Optional"""
    return typing.get_origin(ty) is typing.Union and typing.get_args(ty)[1] is type(None)


def get_optional_type(ty: Any) -> type:
    """get the `T` from a typing.Optional[T]"""
    return typing.get_args(ty)[0]


def is_list(ty: Any) -> bool:
    """determines if `ty` is typing.List or list"""
    return typing.get_origin(ty) is list or ty is list


def get_list_type(ty: Any) -> type:
    """get the generic type of a typing.List[T] or list[T]"""
    return typing.get_args(ty)[0]


def is_config_type(ty: Any) -> bool:
    """determines if ty is a subclass of ConfigType"""
    try:
        return issubclass(ty, ConfigType)
    except TypeError:
        return False


def is_config_class(ty: Any) -> bool:
    """determines if ty is a subclass of ConfigClass"""
    try:
        return issubclass(ty, ConfigClass)
    except TypeError:
        return False


def get_type_args(ty: Any) -> Dict[str, Any]:
    """
    The type of a field implies several options for its argument parsing,
    such as `type`, `nargs`, and `required`

    For example a parameter of type Option[T] is parsed as `T`,
    has no implication on `nargs`, and is not required

    Similarly a parameter of List[T] is also parsed as `T`,
    might have `nargs="*"`, and has no implication on `required`

    This function determines these settings based upon a given type.
    """

    # bools have special parsing so that they behave as users expect
    if ty is bool:
        return {"type": parse_bool}

    # for subclasses of ConfigType, we use the .from_string() method
    if is_config_type(ty):
        return {"type": ty.from_string}

    # recursing handles the case where e.g. the inner type is a bool
    # or a subclass of ConfigType, both of which need special handling
    #
    # parameters with this type are optional so we set `required=False`
    if is_optional(ty):
        opts = get_type_args(get_optional_type(ty))
        return {"type": opts["type"], "required": False}

    # `nargs="*"` is a reasonable default for lists that be overriden by the user
    # if other behaviour is desired
    if is_list(ty):
        opts = get_type_args(get_list_type(ty))
        return {"type": opts["type"], "nargs": "*"}

    # the default, just use the type as-is
    return {"type": ty}
