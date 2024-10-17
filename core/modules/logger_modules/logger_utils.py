import logging
from typing import Optional

def get_logger(name: str, log_file: Optional[str] = None, log_level: int = logging.DEBUG) -> logging.Logger:
    """Utility to get a configured logger with optional file logging and custom log level."""
    logger = logging.getLogger(name)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)  # Console handler with INFO level

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    # Add the console handler to the logger
    logger.addHandler(console_handler)

    # If log_file is provided, set up file logging
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)  # File logging with DEBUG level
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Set log level for the logger (default is DEBUG)
    logger.setLevel(log_level)
    logger.propagate = False  # Prevent duplicate logging

    return logger
