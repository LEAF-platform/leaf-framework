from abc import abstractmethod
import threading, time, logging
from typing import Optional, Callable, List, Dict
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="input_module.log", 
                    log_level=logging.DEBUG)

class PollingWatcher(EventWatcher):
    """
    A base class for watchers that perform periodic 
    polling to check for events, supporting start, stop, 
    and measurement callbacks.
    """
    def __init__(self, interval: int, metadata_manager: MetadataManager,
                 callbacks: Optional[List[Callable]] = None, 
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialise PollingWatcher.

        Args:
            interval (int): Polling interval in seconds.
            metadata_manager (MetadataManager): Manages equipment 
                                                metadata.
            callbacks (Optional[List[Callable]]): Callbacks for event 
                                                updates.
            error_holder (Optional[ErrorHolder]): Optional object to 
                                                manage errors.
        """
        term_map = {
            self.start_message: metadata_manager.experiment.start,
            self.stop_message: metadata_manager.experiment.stop,
            self.measurement_message: metadata_manager.experiment.measurement
        }

        super().__init__(term_map, metadata_manager, callbacks=callbacks,
                         error_holder=error_holder)
        logger.debug("Interval: %s", interval)
        self.interval = interval
        self.running = False
        self._thread = None

    def start_message(self,data):
        return self._dispatch_callback(self.start_message,data)
    
    def stop_message(self,data):
        return self._dispatch_callback(self.stop_message,data)
    
    def measurement_message(self,data):
        return self._dispatch_callback(self.measurement_message,data)
    
    @abstractmethod
    def _fetch_data(self) -> Dict[str, Optional[dict]]:
        """
        Abstract method for protocol-specific data fetching logic.
        Returns a dictionary with potential data for 'start', 'stop',
        and 'measurement' events. Subclasses must implement this.

        Returns:
            Dict[str, Optional[dict]]: A dictionary containing data for
                                        each event type as available.
            Example: {"measurement": data, "start": None, "stop": data}
        """
        return {"measurement": None, "start": None, "stop": None}

    def _poll(self) -> None:
        """
        Poll data at regular intervals and trigger the appropriate
        callbacks if new data is available.
        """
        while self.running:
            data = self._fetch_data()
            if data.get("measurement") is not None:
                self.measurement_message(data["measurement"])
            if data.get("start") is not None:
                self.start_message(data["start"])
            if data.get("stop") is not None:
                self.stop_message(data["stop"])
            time.sleep(self.interval)

    def start(self) -> None:
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

    def stop(self) -> None:
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

