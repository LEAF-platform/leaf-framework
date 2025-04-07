import os
import logging
from typing import Optional, List, Callable

from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf.utility.logger.logger_utils import get_logger
from leaf.error_handler.error_holder import ErrorHolder
from leaf_register.metadata import MetadataManager

logger = get_logger(__name__, log_file="input_module.log", 
                    log_level=logging.DEBUG)

class ExternalEventWatcher(EventWatcher):
    def __init__(self, 
                 metadata_manager: MetadataManager = None,
                 callbacks: Optional[List[Callable]] = None, 
                 error_holder: Optional[ErrorHolder] = None) -> None:

        super().__init__(metadata_manager, callbacks=callbacks, 
                         error_holder=error_holder)
        


