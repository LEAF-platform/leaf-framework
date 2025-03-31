import logging
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf.utility.logger.logger_utils import get_logger
from leaf_register.metadata import MetadataManager
logger = get_logger(__name__, log_file="input_module.log", log_level=logging.DEBUG)

class DBWatcher(EventWatcher):
    def __init__(self, metadata_manager: MetadataManager, 
                 callbacks = None, error_holder=None):
        raise NotImplementedError()
        term_map = {}
        super(FileWatcher, self).__init__(term_map,metadata_manager,
                                          callbacks=callbacks,
                                          error_holder=error_holder)
        
    def start(self):
        raise NotImplementedError()
