from typing import Optional, Callable, List, Dict

from leaf_register.metadata import MetadataManager
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.error_handler.error_holder import ErrorHolder

class SimpleWatcher(PollingWatcher):        
    """
    A concrete implementation of PollingWatcher that uses
    predefined fetchers to retrieve and monitor data.
    """
    def __init__(self, metadata_manager: MetadataManager, interval: int,
                 callbacks: Optional[List[Callable]] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialise SimpleWatcher.

        Args:
            metadata_manager (MetadataManager): Manages equipment 
                                                metadata.
            interval (int): Polling interval in seconds.
            callbacks (Optional[List[Callable]]): Callbacks for event 
                                                  updates.
            error_holder (Optional[ErrorHolder]): Optional object to 
                                                  manage errors.
        """
        super().__init__(interval, metadata_manager, 
                         callbacks=callbacks,
                         error_holder=error_holder)
        
        self._interval = interval
        self._metadata_manager = metadata_manager

    def _fetch_data(self) -> Dict[str, Dict[str, str]]:
        """
        Fetch dummy data for testing and triggering callbacks.

        Returns:
            Dict[str, Dict[str, str]]: Example data to 
                                       simulate event triggers.
        """
        return {"measurement": {"data": "data"}}