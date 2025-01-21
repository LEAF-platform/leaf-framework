import logging
from requests import Response, get, RequestException
from typing import Optional, Callable, List, Dict
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="input_module.log", 
                    log_level=logging.DEBUG)

class URLState:
    """
    Tracks URL state, including ETag, Last-Modified, and previous data,
    to detect changes in API responses.
    """
    def __init__(self, url_type: str):
        """
        Initialize URLState.

        Args:
            url_type: Description of URL type.
        """
        self.url_type = url_type
        self.etag = None
        self.last_modified = None
        self.previous_data = None

    def update_from_response(self, response: Response) -> Optional[dict]:
        """
        Update state from an API response and return new data if a 
        change is detected, or None if no change is detected.

        Args:
            response: HTTP response from the API.

        Returns:
            New JSON data if a change is detected, else None.
        """
        if "ETag" in response.headers and response.headers["ETag"] == self.etag:
            return None
        if (
            "Last-Modified" in response.headers
            and response.headers["Last-Modified"] == self.last_modified
        ):
            return None

        current_data = response.json()
        if self.etag is None and self.last_modified is None:
            if current_data == self.previous_data:
                return None

        self.etag = response.headers.get("ETag")
        self.last_modified = response.headers.get("Last-Modified")
        self.previous_data = current_data
        return current_data


class HTTPWatcher(PollingWatcher):
    """
    Polls API endpoints at specified intervals, utilizing ETag and
    Last-Modified headers to detect changes. Supports measurement,
    start, and stop conditions.
    """
    def __init__(
        self, metadata_manager: MetadataManager,
        measurement_url: str,
        start_url: Optional[str] = None,
        stop_url: Optional[str] = None,
        interval: int = 60,
        headers: Optional[Dict[str, str]] = None, 
        callbacks: Optional[List[Callable]] = None,
        error_holder: Optional[ErrorHolder] = None
    ) -> None:
        """
        Initialize HTTPWatcher.

        Args:
            metadata_manager (MetadataManager): Manages equipment 
                                                metadata.
            measurement_url (str): URL to poll for measurement data.
            start_url (Optional[str]): Optional URL for start condition 
                                    polling.
            stop_url (Optional[str]): Optional URL for stop condition 
                                    polling.
            interval (int): Polling interval in seconds.
            headers (Optional[Dict[str, str]]): Custom headers for API 
                                                requests.
            callbacks (Optional[List[Callable]]): Callbacks for event 
                                                  updates.
            error_holder (Optional[ErrorHolder]): Optional object to 
                                                  manage errors.
        """
        super().__init__(interval, metadata_manager, callbacks=callbacks,
                         error_holder=error_holder)

        self.url_states = {"measurement": URLState("measurement")}
        self.urls = {"measurement": measurement_url}

        if start_url:
            self.url_states["start"] = URLState("start")
            self.urls["start"] = start_url

        if stop_url:
            self.url_states["stop"] = URLState("stop")
            self.urls["stop"] = stop_url

        self._headers = headers or {}

    def _fetch_data(self) -> Dict[str, Optional[dict]]:
        """
        Poll each configured URL for changes and return data if updates 
        are detected.

        Returns:
            Dict[str, Optional[dict]]: A dictionary with new data 
                                        for each URL.
        """
        fetched_data = {"measurement": None, "start": None, "stop": None}

        for url_key, url in self.urls.items():
            state = self.url_states[url_key]
            headers = self._headers.copy()

            if state.etag:
                headers["If-None-Match"] = state.etag
            if state.last_modified:
                headers["If-Modified-Since"] = state.last_modified

            try:
                response = get(url, headers=headers)
                response.raise_for_status()
            except RequestException as e:
                logger.error(f"Error polling API {url}: {e}")
                continue

            if response.status_code == 200:
                new_data = state.update_from_response(response)
                if new_data is not None:
                    fetched_data[url_key] = new_data
        return fetched_data
