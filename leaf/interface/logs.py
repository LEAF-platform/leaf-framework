import asyncio
import logging
import time
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


def create_logs_panel() -> None:
    # Your custom UI log object and handler
    log = ui.log(max_lines=1000).classes('flex-grow h-[60vh] overflow-auto')
    handler = LogElementHandler(log)

    # Define the log format you want (including date, class, level, and message)
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    # Set the formatter for the handler
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    
    # Add the handler to all available loggers
    for logger_name in logging.root.manager.loggerDict:
        logger = logging.getLogger(logger_name)

        # Check if the logger already has handlers to avoid adding the same handler multiple times
        if not logger.hasHandlers():
            logger.addHandler(handler)
        else:
            # Optional: Remove existing handler if it's the same type and re-add your custom handler
            for existing_handler in logger.handlers:
                if isinstance(existing_handler, LogElementHandler):
                    logger.removeHandler(existing_handler)
                    break
            logger.addHandler(handler)

    # Log an initial message to indicate log collection is starting
    logger = logging.getLogger()  # This will use the root logger
    logger.info("Collecting logs up to 1000 lines")

    # Clean up the handler when the client disconnects
    ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))

    # # Async function to update the time
    # async def update_time() -> None:
    #     while True:
    #         log.push(f"Time: {datetime.now()}")
    #         # List all available loggers
    #         loggers = logging.root.manager.loggerDict
    #
    #         # Print all logger names to find the relevant one
    #         for logger_name in loggers:
    #             log.push(logger_name)
    #         await asyncio.sleep(1)  # Non-blocking sleep
    #
    # # Run the async function
    # asyncio.create_task(update_time())  # Start the async task
