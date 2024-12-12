import os
import logging
from typing import Optional

log_dir = "logs"

def get_logger(name: str, log_file: Optional[str] = None, 
               log_level: int = logging.DEBUG, 
               error_log_file: Optional[str] = None) -> logging.Logger:
    """
    Utility to get a configured logger with optional file logging and custom log level.
    Supports separate log files for different log levels.
    
    Args:
        name: Name of the logger.
        log_file: Log file for general logging.
        log_level: Logging level for general logging.
        error_log_file: Log file for error-specific logging.
    
    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)  
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)    
    logger.addHandler(console_handler)

    
    if log_file:
        os.makedirs(log_dir, exist_ok=True)
        general_log_file = os.path.join(log_dir, log_file)
        file_handler = logging.FileHandler(general_log_file)
        file_handler.setLevel(log_level)  
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if error_log_file:
        error_log_file_path = os.path.join(log_dir, error_log_file)
        error_file_handler = logging.FileHandler(error_log_file_path)
        error_file_handler.setLevel(logging.ERROR)  
        error_file_handler.setFormatter(formatter)
        logger.addHandler(error_file_handler)
    logger.setLevel(logging.DEBUG)  

    return logger


def set_log_dir(directory: str) -> None:
    global log_dir
    os.makedirs(directory, exist_ok=True)
    log_dir = directory
