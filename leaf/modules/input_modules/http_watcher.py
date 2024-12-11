import requests, logging
from typing import Optional, Callable, List, Dict
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.leaf_register.metadata import MetadataManager

logger = logging.getLogger(__name__)

class URLState:
    """
    Tracks URL state, including ETag, Last-Modified, and previous data,
    to detect changes in API responses.
    """
    def __init__(self, url_type: str):
        """
        Initialize URLState.

        Args:
            url_type: Description of URL type (e.g., 'measurement', 'start', 'stop').
        """
        self.url_type = url_type
        self.etag = None
        self.last_modified = None
        self.previous_data = None

    def update_from_response(self, response: requests.Response) -> Optional[dict]:
        """
        Update state from an API response and return new data if a change is
        detected, or None if no change is detected.

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
        self,
        metadata_manager: MetadataManager,
        measurement_url: str,
        start_url: Optional[str] = None,
        stop_url: Optional[str] = None,
        interval: int = 60,
        initialise_callbacks: Optional[List[Callable]] = None,
        start_callbacks: Optional[List[Callable]] = None,
        measurement_callbacks: Optional[List[Callable]] = None,
        stop_callbacks: Optional[List[Callable]] = None,
        headers: Optional[dict] = None,
    ) -> None:
        """
        Initialize APIWatcher.

        Args:
            metadata_manager: Manages equipment metadata.
            measurement_url: URL to poll for measurement data.
            start_url: Optional URL for start condition polling.
            stop_url: Optional URL for stop condition polling.
            interval: Polling interval in seconds.
            initialise_callbacks: Callbacks for initialization.
            start_callbacks: Callbacks for start events.
            measurement_callbacks: Callbacks for measurement events.
            stop_callbacks: Callbacks for stop events.
            headers: Custom headers for API requests.
        """
        super().__init__(
            metadata_manager,
            interval,
            initialise_callbacks=initialise_callbacks,
            measurement_callbacks=measurement_callbacks,
            start_callbacks=start_callbacks,
            stop_callbacks=stop_callbacks,
        )

        
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
        Poll each configured URL for changes and return data if updates are detected.
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
                response = requests.get(url, headers=headers)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Error polling API {url}: {e}")
                continue

            if response.status_code == 200:
                new_data = state.update_from_response(response)
                if new_data is not None:
                    fetched_data[url_key] = new_data
        return fetched_data
