from scaler.config.section.webui import WebUIConfig
from scaler.ui.webui import start_webui


def main():
    start_webui(WebUIConfig.parse("Web UI for Scaler Monitoring", "webui"))
