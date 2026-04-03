# PYTHON_ARGCOMPLETE_OK
from typing import Optional

from scaler.config.section.webgui import WebGUIConfig

try:
    from scaler.ui.webgui import start_webgui
except ModuleNotFoundError as error:
    raise ModuleNotFoundError("GUI dependencies are missing. Please run: pip install 'opengris-scaler[gui]'") from error


def main(config: Optional[WebGUIConfig] = None) -> None:
    if config is None:
        config = WebGUIConfig.parse("Web GUI for Scaler Monitoring", "gui")

    start_webgui(config)
