import logging

import uvicorn  # pyright: ignore[reportMissingImports]

from scaler.config.section.webgui import WebGUIConfig
from scaler.ui.app import create_app
from scaler.utility.process_bootstrap import bootstrap_process

logger = logging.getLogger(__name__)


def start_webgui(config: WebGUIConfig) -> None:
    bootstrap_process(
        config.logging_config.paths, config.logging_config.config_file, config.logging_config.level, process_name="gui"
    )

    app = create_app(config)
    logger.info(f"Web GUI is now listening on: http://{config.gui_address}")
    uvicorn.run(app, host=config.gui_address.host, port=config.gui_address.port)
