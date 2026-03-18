import uvicorn

from scaler.config.section.webui import WebUIConfig
from scaler.ui.app import create_app
from scaler.utility.logging.utility import setup_logger


def start_webui(config: WebUIConfig):
    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    app = create_app(config)
    uvicorn.run(app, host=config.web_host, port=config.web_port)
