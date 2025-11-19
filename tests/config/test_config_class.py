import dataclasses
import unittest
from typing import Dict, List, Optional, Tuple
from unittest.mock import mock_open, patch

from scaler.config.config_class import ConfigClass, parse_bool
from scaler.config.mixins import ConfigType

try:
    from typing import override  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import override  # type: ignore[attr-defined]

try:
    from typing import Self  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Self  # type: ignore[attr-defined]


class MockArgParser:
    prog_name: str
    args: List[Tuple[Tuple, Dict]]

    init_args: Tuple
    init_kwargs: Dict

    def __init__(self, prog_name: str, *args, **kwargs) -> None:
        self.prog_name = prog_name
        self.args = []
        self.init_args = args
        self.init_kwargs = kwargs

    def add_argument(self, *args, **kwargs) -> None:
        self.args.append((args, kwargs))


class TestConfigClass(unittest.TestCase):
    """Tests the behavior of ConfigClass"""

    @patch("scaler.config.config_class.ArgParser", MockArgParser)
    def test_config_class(self) -> None:
        def parse_hex(s: str) -> int:
            return int(s, 16)

        class MyConfigType(ConfigType):
            _s: str

            def __init__(self, s: str) -> None:
                self._s = s

            @override
            @classmethod
            def from_string(cls, value: str) -> Self:
                return cls(value)

            @override
            def __str__(self) -> str:
                return self._s

        @dataclasses.dataclass
        class MyConfig(ConfigClass):
            my_int: int
            positional_one: str = dataclasses.field(metadata=dict(positional=True))
            positional_two: str = dataclasses.field(metadata=dict(positional=True))

            config_type: MyConfigType = dataclasses.field(metadata=dict(short="-ct", help="help"))

            optional_int: Optional[int]
            optional_config_type: Optional[MyConfigType]

            a_bool: bool
            flag: bool = dataclasses.field(metadata=dict(action="store_true"))

            list_one: List[int]
            list_two: List[int] = dataclasses.field(metadata=dict(nargs="+"))

            custom_type: int = dataclasses.field(metadata=dict(type=parse_hex))

            with_default: int = 42

            passthrough: int = dataclasses.field(
                default=-1, metadata=dict(any=0, field=1, can=2, be=3, passed=4, through=5)
            )

            @override
            @staticmethod
            def section_name() -> str:
                return "my_config"

            @override
            @staticmethod
            def program_name() -> str:
                return "this is a test config"

        parser: MockArgParser = MyConfig.parser()

        self.assertEqual(parser.prog_name, "this is a test config")
        self.assertEqual(parser.init_kwargs["config_file_parser_class"].sections, ["my_config"])

        # first arg is the config file, drop it
        args = parser.args[1:]
        self.assertEqual(args[0], (("--my-int",), {"type": int}))
        self.assertEqual(args[1], (("positional-one",), {"type": str}))
        self.assertEqual(args[2], (("positional-two",), {"type": str}))
        self.assertEqual(args[3], (("--config-type", "-ct"), {"type": MyConfigType.from_string, "help": "help"}))
        self.assertEqual(args[4], (("--optional-int",), {"type": int, "required": False}))
        self.assertEqual(args[5], (("--optional-config-type",), {"type": MyConfigType.from_string, "required": False}))
        self.assertEqual(args[6], (("--a-bool",), {"type": parse_bool}))
        self.assertEqual(args[7], (("--flag",), {"action": "store_true"}))
        self.assertEqual(args[8], (("--list-one",), {"type": int, "nargs": "*"}))
        self.assertEqual(args[9], (("--list-two",), {"type": int, "nargs": "+"}))
        self.assertEqual(args[10], (("--custom-type",), {"type": parse_hex}))
        self.assertEqual(args[11], (("--with-default",), {"type": int, "default": 42}))
        self.assertEqual(
            args[12],
            (
                ("--passthrough",),
                {"type": int, "default": -1, "any": 0, "field": 1, "can": 2, "be": 3, "passed": 4, "through": 5},
            ),
        )

    @patch("sys.argv", ["script"])
    def test_empty(self) -> None:
        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            @override
            @staticmethod
            def section_name() -> str:
                return "my_config"

            @override
            @staticmethod
            def program_name() -> str:
                return "this is a test config"

        MyConfigClass.parse()

    @patch("sys.argv", ["script", "--config", "file", "--command-line", "99"])
    @patch.dict("os.environ", {"ENV_VAR_ONE": "99", "ENV_VAR_TWO": "98"})
    @patch(
        "builtins.open",
        mock_open(
            read_data="""
            [my_config]
            config-file = 99

            [unused_section]
            another-one = 97
            """
        ),
    )
    def test_precedence(self) -> None:
        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            default: int = dataclasses.field(default=0)
            config_file: int = dataclasses.field(default=1)
            env_var: int = dataclasses.field(default=2, metadata=dict(env_var="ENV_VAR_ONE"))
            command_line: int = dataclasses.field(default=3, metadata=dict(env_var="ENV_VAR_TWO"))

            @override
            @staticmethod
            def section_name() -> str:
                return "my_config"

            @override
            @staticmethod
            def program_name() -> str:
                return "this is a test config"

        config = MyConfigClass.parse()

        self.assertEqual(config, MyConfigClass(default=0, config_file=99, env_var=99, command_line=99))
