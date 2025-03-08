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
    log = ui.log(max_lines=100).classes('w-full')
    handler = LogElementHandler(log)
    logger.addHandler(handler)
    ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))

    with ui.tab_panel(logs_tab).style('width: 100%'):
        ui.label('System Logs').classes('text-xl font-bold')
        ui.number(label='Max log lines', value=10,
            on_change=lambda e: setattr(log, 'max_lines', int(e.value)))
