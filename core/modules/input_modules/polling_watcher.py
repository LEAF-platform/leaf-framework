from abc import abstractmethod
import threading, time, logging
from typing import Optional, Callable, List, Dict, Any
from core.modules.input_modules.event_watcher import EventWatcher
from core.metadata_manager.metadata import MetadataManager

logger = logging.getLogger(__name__)

class PollingWatcher(EventWatcher):
    """
    A base class for watchers that perform periodic 
    polling to check for events, supporting start, stop, 
    and measurement callbacks.
    """
    def __init__(self, metadata_manager: MetadataManager, interval: int,
                 initialise_callbacks: Optional[List[Callable]] = None,
                 measurement_callbacks: Optional[List[Callable]] = None,
                 start_callbacks: Optional[List[Callable]] = None,
                 stop_callbacks: Optional[List[Callable]] = None) -> None:
        """
        Initialize PollingWatcher.

        Args:
            metadata_manager: Manages equipment metadata.
            interval: Polling interval in seconds.
            initialise_callbacks: Callbacks for initialization events.
            measurement_callbacks: Callbacks for measurement events.
            start_callbacks: Callbacks for start events.
            stop_callbacks: Callbacks for stop events.
        """
        logger.debug("Initialising PollingWatcher...")
        super().__init__(metadata_manager,
                         initialise_callbacks=initialise_callbacks,
                         measurement_callbacks=measurement_callbacks,
                         start_callbacks=start_callbacks,
                         stop_callbacks=stop_callbacks)
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
        logger.debug("Fetching data...")
        return {"measurement": None, "start": None, "stop": None}

    def _poll(self):
        """
        Poll data at regular intervals and trigger the appropriate callbacks if new data is available.
        """
        logger.info("Polling started.")
        while self.running:
            logger.debug("Polling for data... is running")
            data = self._fetch_data()
            if data.get("measurement") is not None:
                self._initiate_callbacks(self.measurement_callbacks, data["measurement"])
            if data.get("start") is not None:
                self._initiate_callbacks(self.start_callbacks, data["start"])
            if data.get("stop") is not None:
                self._initiate_callbacks(self.stop_callbacks, data["stop"])
            logger.debug(f"Polling for data... is sleeping for {self.interval} seconds")
            time.sleep(self.interval)
        logger.info("Polling stopped.")

    def start(self):
        """
        Start the PollingWatcher in a separate thread and 
        trigger initialization callbacks.
        """
        logger.info("Starting PollingWatcher...")
        if self.running:
            logger.warning("PollingWatcher is already running.")
            return
        logger.debug("Setting thread...")
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
