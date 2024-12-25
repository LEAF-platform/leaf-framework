from leaf_register.metadata import MetadataManager
from leaf.modules.input_modules.polling_watcher import PollingWatcher

class SimpleWatcher(PollingWatcher):        
    def __init__(self, metadata_manager: MetadataManager, interval: int,
                 callbacks=None,error_holder=None):
        super().__init__(interval,metadata_manager,callbacks=callbacks,
                         error_holder=error_holder)
        self._interval = interval
        self._metadata_manager = metadata_manager

    def _fetch_data(self) -> dict[str, dict[str, str]]:
        # dummy data to trigger the interpreter
        return {"measurement": {"data": "data"}}