import logging
from datetime import datetime

from nicegui import ui


class LogElementHandler(logging.Handler):
    """A logging handler that emits messages to a log element."""

    def __init__(self, element: ui.log, level: int = logging.NOTSET) -> None:
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.element.push(msg)
        except Exception:
            self.handleError(record)


def create_logs_panel(tabs, logs_tab, self) -> None:
    logger = logging.getLogger()
    log = ui.log(max_lines=1000).classes('flex-grow h-[60vh] overflow-auto')
    handler = LogElementHandler(log)
    logger.addHandler(handler)
    logger.info("Collecting logs up to 1000 lines")
    ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))