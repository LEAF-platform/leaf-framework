from abc import abstractmethod
import threading, time, logging
from typing import Optional, Callable, List, Dict, Any
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf_register.metadata import MetadataManager

logger = get_logger(__name__, log_file="input_module.log", log_level=logging.DEBUG)

class PollingWatcher(EventWatcher):
    """
    A base class for watchers that perform periodic 
    polling to check for events, supporting start, stop, 
    and measurement callbacks.
    """
    def __init__(self,interval: int, metadata_manager: MetadataManager,
                 callbacks = None, error_holder=None):
        """
        Initialise PollingWatcher.

        Args:
            metadata_manager: Manages equipment metadata.
            interval: Polling interval in seconds.
            initialise_callbacks: Callbacks for initialization events.
            measurement_callbacks: Callbacks for measurement events.
            start_callbacks: Callbacks for start events.
            stop_callbacks: Callbacks for stop events.
        """
        term_map = {self.start : metadata_manager.experiment.start,
                    self.stop : metadata_manager.experiment.stop,
                    self.measurement : metadata_manager.experiment.measurement}
        
        super().__init__(term_map,metadata_manager,
                             callbacks=callbacks,
                             error_holder=error_holder)
        logger.debug("Interval: %s", interval)
        self.interval = interval
        self.running = False
        self._thread = None

    @abstractmethod
    def _fetch_data(self) -> Dict[str, Optional[dict[str, Any]]]:
        """
        Abstract method for protocol-specific data fetching logic. Returns
        a dictionary with potential data for 'start', 'stop', and 
        'measurement' events. Subclasses must implement this.

        Returns:
            A dictionary containing data for each event type as available.
            Example: {"measurement": data, "start": None, "stop": data}
        """
        return {"measurement": None, "start": None, "stop": None}

    
    def start(self,data):
        return self._dispatch_callback(self.start,data)
    
    def stop(self,data):
        return self._dispatch_callback(self.stop,data)
    
    def measurement(self,data):
        return self._dispatch_callback(self.measurement,data)

    def _poll(self):
        """
        Poll data at regular intervals and trigger the appropriate callbacks if new data is available.
        """
        while self.running:
            data = self._fetch_data()
            if data.get("measurement") is not None:
                self.start(data["measurement"])
            if data.get("start") is not None:
                self.start(data["start"])
            if data.get("stop") is not None:
                self.start(data["stop"])
            time.sleep(self.interval)

    def start(self):
        """
        Start the PollingWatcher in a separate thread and 
        trigger initialization callbacks.
        """
        logger.info("Starting PollingWatcher...")
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._poll)
        self._thread.daemon = True
        self._thread.start()
        super().start()
        
    def stop(self):
        """
        Stop the PollingWatcher and wait for the thread to terminate.
        """
        logger.info("Stopping PollingWatcher...")
        if not self.running:
            logger.warning("PollingWatcher is not running.")
            return
        self.running = False
        if self._thread:
            self._thread.join()
        logger.info("PollingWatcher stopped.")
