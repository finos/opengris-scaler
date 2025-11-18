from packaging.version import parse

try:
    from nicegui.version import __version__
except ImportError as e:
    raise ImportError("Could not determine NiceGUI version. Is it installed?") from e

NICEGUI_VERSION = parse(__version__)
NICEGUI_MAJOR_VERSION = NICEGUI_VERSION.major
