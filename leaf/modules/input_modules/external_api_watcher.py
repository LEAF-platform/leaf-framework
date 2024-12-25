from typing import Callable, Dict, Optional, List
import logging
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf_register.metadata import MetadataManager

logger = get_logger(__name__, log_file="input_module.log", log_level=logging.DEBUG)


class APIState:
    """
    Tracks the state of an API data type (e.g., 'measurement', 'start',
    'stop') to detect changes and avoid redundant callback triggers.
    """

    def __init__(self, api_type: str):
        self.api_type = api_type
        self.previous_data = None

    def update_if_new(self, data: Optional[dict]) -> Optional[dict]:
        """
        Update the state with new data if it's different from the
        last known data.

        Args:
            data: The new data fetched from the API.

        Returns:
            The new data if it has changed; otherwise, None.
        """
        if data != self.previous_data:
            self.previous_data = data
            return data
        return None


class ExternalApiWatcher(PollingWatcher):
    """
    Polls external APIs at specified intervals, using different
    data-fetching functions for measurement, start, and stop conditions.
    Only triggers callbacks when new data is detected.
    """        
    def __init__(self,metadata_manager: MetadataManager,
                 measurement_fetcher: Callable[[], Optional[dict]],
                 start_fetcher: Optional[Callable[[], Optional[dict]]] = None,
                 stop_fetcher: Optional[Callable[[], Optional[dict]]] = None,
                 interval: int = 60,callbacks = None, error_holder=None) -> None:
        """
        Initialise ExternalApiWatcher.

        Args:
            metadata_manager: Manages equipment metadata.
            measurement_fetcher: Function to fetch measurement data.
            start_fetcher: Optional function to fetch start event data.
            stop_fetcher: Optional function to fetch stop event data.
            interval: Polling interval in seconds.
        """
        print(type(metadata_manager))
        super().__init__(interval,metadata_manager,callbacks=callbacks,
                         error_holder=error_holder)
        self.fetchers = {
            "measurement": measurement_fetcher,
            "start": start_fetcher,
            "stop": stop_fetcher,
        }
        self.api_states = {
            key: APIState(key) for key in 
            self.fetchers.keys() if self.fetchers[key]
        }

    def _fetch_data(self) -> Dict[str, Optional[dict]]:
        """
        Poll each configured fetcher for changes and return data if updates
        are detected.

        Returns:
            A dictionary containing only new data for each condition
            type if updates are detected.
        """
        fetched_data = {"measurement": None, "start": None, "stop": None}

        for key, fetcher in self.fetchers.items():
            if fetcher:
                try:
                    data = fetcher()
                    if data is not None:
                        new_data = self.api_states[key].update_if_new(data)
                        fetched_data[key] = new_data
                except Exception as e:
                    logger.error(f"Error fetching data for {key}: {e}")

        return fetched_data
