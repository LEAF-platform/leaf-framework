import logging

from typing import Optional
from typing import Callable
from typing import List
from typing import Dict
from typing import Any

from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.utility.logger.logger_utils import get_logger
from leaf.error_handler.error_holder import ErrorHolder
from leaf_register.metadata import MetadataManager

logger = get_logger(__name__, log_file="input_module.log", 
                    log_level=logging.DEBUG)


class APIState:
    """
    Tracks the state of a specific API event type
    (e.g., 'measurement', 'start', 'stop') to avoid 
    redundant callback triggers.
    """

    def __init__(self, api_type: str) -> None:
        """
        Initialize APIState.

        Args:
            api_type (str): The API event type this state is tracking.
        """
        self.api_type: str = api_type
        self.previous_data: Optional[dict] = None

    def update_if_new(self, data: Optional[dict]) -> Optional[dict]:
        """
        Update internal state only if new data differs from the previous one.

        Args:
            data (Optional[dict]): The latest API data.

        Returns:
            Optional[dict]: The data if it's new; otherwise, None.
        """
        if data != self.previous_data:
            self.previous_data = data
            return data
        return None


class ExternalApiWatcher(PollingWatcher):
    """
    A polling-based watcher for external APIs.

    Uses separate fetch functions for 'measurement', 'start', and 'stop'.
    Only dispatches new data if it has changed from the previous poll.
    """

    def __init__(
        self,
        metadata_manager: MetadataManager,
        measurement_fetcher: Callable[[], Optional[dict]],
        start_fetcher: Optional[Callable[[], Optional[dict]]] = None,
        stop_fetcher: Optional[Callable[[], Optional[dict]]] = None,
        interval: int = 60,
        callbacks: Optional[List[Callable[[str, Any], None]]] = None,
        error_holder: Optional[ErrorHolder] = None
    ) -> None:
        """
        Initialize ExternalApiWatcher.

        Args:
            metadata_manager (MetadataManager): Metadata manager.
            measurement_fetcher (Callable): Fetcher for measurement data.
            start_fetcher (Optional[Callable]): Optional fetcher for start data.
            stop_fetcher (Optional[Callable]): Optional fetcher for stop data.
            interval (int): Polling interval in seconds.
            callbacks (Optional[List[Callable]]): Callback functions to run on data.
            error_holder (Optional[ErrorHolder]): Error tracking utility.
        """
        super().__init__(
            interval=interval,
            metadata_manager=metadata_manager,
            callbacks=callbacks,
            error_holder=error_holder
        )

        self._fetchers: Dict[str, Optional[Callable[[], Optional[dict]]]] = {
            "measurement": measurement_fetcher,
            "start": start_fetcher,
            "stop": stop_fetcher
        }

        self._api_states: Dict[str, APIState] = {
            key: APIState(key)
            for key, fetcher in self._fetchers.items()
            if fetcher is not None
        }

    def _fetch_data(self) -> Dict[str, Optional[dict]]:
        """
        Poll all configured fetchers and return new data (if any).

        Returns:
            dict: Contains 'measurement', 'start', 'stop' with any new data found.
        """
        results: Dict[str, Optional[dict]] = {
            "measurement": None,
            "start": None,
            "stop": None
        }

        for key, fetcher in self._fetchers.items():
            if fetcher:
                try:
                    data = fetcher()
                    if data:
                        new_data = self._api_states[key].update_if_new(data)
                        results[key] = new_data
                except Exception as e:
                    logger.error(f"[ExternalApiWatcher] Failed to fetch '{key}': {e}", exc_info=True)

        return results
