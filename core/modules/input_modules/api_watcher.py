import time
import requests
import logging
import threading
from typing import Optional, Callable, List

from core.modules.logger_modules.logger_utils import get_logger
from core.modules.input_modules.event_watcher import EventWatcher
from core.metadata_manager.metadata import MetadataManager

default_header = {}
logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class URLState:
    """
    Manages the state of a URL being polled, tracking ETag, Last-Modified,
    and the previous response data to detect changes in API responses.
    """
    def __init__(self, url_type: str):
        """
        Initialise the URLState.

        Args:
            url_type: A string describing the 
            type of URL (e.g., 'measurement', 'start', 'stop').
        """
        self.url_type = url_type  
        self.etag = None  
        self.last_modified = None  
        self.previous_data = None  

    def update_from_response(self, response: requests.Response) -> Optional[dict]:
        """
        Update the state from the API response and 
        return the new data if a change is detected.
        Return None if no change is detected.

        Args:
            response: The HTTP response from the API.

        Returns:
            The new JSON data if a change is detected, otherwise None.
        """
        if ("ETag" in response.headers and 
            response.headers["ETag"] == self.etag):
            return None  
        if ("Last-Modified" in response.headers and 
            response.headers["Last-Modified"] == self.last_modified):
            return None  
        current_data = response.json()
        if self.etag is None and self.last_modified is None:
            if current_data == self.previous_data:
                return None  
        self.etag = response.headers.get("ETag", None)
        self.last_modified = response.headers.get("Last-Modified", None)
        self.previous_data = current_data
        return current_data  


class APIWatcher(EventWatcher):
    """
    A Watcher class that polls API routes every interval. 
    Attempts to use ETag and Last-Modified which are sent in the header 
    to reduce comparison of large datasets. Fallbacks to comparing 
    last request to current request if the API being called 
    doesn't support it. Has one required callback and url (measurement) 
    with optional start and stop.
    """ 
    def __init__(self, metadata_manager: MetadataManager,
                 measurement_url: str,
                 start_url: Optional[str] = None,
                 stop_url: Optional[str] = None,
                 interval: int = 60,
                 start_callbacks: Optional[Callable] = None,
                 measurement_callbacks: Optional[Callable] = None,
                 stop_callbacks: Optional[Callable] = None,
                 custom_headers: Optional[dict] = None) -> None:
        """
        Initialise the APIWatcher.

        Args:
            metadata_manager: MetadataManager instance to manage equipment metadata.
            measurement_url: URL to poll for measurement data changes.
            start_url: Optional URL to check when the start condition occurs.
            stop_url: Optional URL to check when the stop condition occurs.
            interval: Time interval in seconds for polling.
            start_callbacks: Callback functions triggered when a start condition is detected.
            measurement_callbacks: Callback functions triggered when measurement data changes.
            stop_callbacks: Callback functions triggered when a stop condition is detected.
            custom_headers: Optional custom headers to include in API requests.
        """
        super().__init__(metadata_manager, measurement_callbacks=measurement_callbacks)
        self.url_states = {"measurement": URLState("measurement")}
        self.urls = {"measurement": measurement_url}

        if start_url:
            self.url_states["start"] = URLState("start")
            self.urls["start"] = start_url
            self._start_callbacks = self._cast_callbacks(start_callbacks)
        else:
            self._start_callbacks = []
        
        if stop_url:
            self.url_states["stop"] = URLState("stop")
            self.urls["stop"] = stop_url
            self._stop_callbacks = self._cast_callbacks(stop_callbacks)
        else:
            self._stop_callbacks = []

        self.interval = interval  
        self.running = False  
        self._thread = None  
        if custom_headers is not None:
            self._custom_headers = custom_headers  
        else:
            self._custom_headers = {}

    def _poll_url(self, url_key: str, callbacks: List[Callable]) -> None:
        """
        Poll a given URL, check for ETag/Last-Modified changes, 
        and trigger callbacks if changes are detected.

        Args:
            url_key: The key for the URL ("measurement", 
                    "start", or "stop").
            callbacks: The list of callbacks to 
                    trigger if a change is detected.
        """
        state = self.url_states[url_key]
        url = self.urls[url_key]
        headers = self._custom_headers.copy()
        
        # Add conditional headers for ETag and Last-Modified
        if state.etag:
            headers["If-None-Match"] = state.etag
        if state.last_modified:
            headers["If-Modified-Since"] = state.last_modified

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  
        except requests.RequestException as e:
            logger.error(f"Error polling API {url}: {e}")
            return None
        
        if response.status_code == 200:
            res = state.update_from_response(response)
            if res is not None:
                self._initiate_callbacks(callbacks, res)
            else:
                logger.debug(f"Polled: {url} but no change detected.")
        
        elif response.status_code == 304:
            logger.debug(f"Polled: {url} but no change detected.")

    def _run(self) -> None:
        """
        Main loop that runs the API watcher, 
        polling all necessary URLs at the 
        specified interval.
        """
        while self.running:
            for url_key, url in self.urls.items():
                if url:
                    callbacks = getattr(self, f"_{url_key}_callbacks")
                    self._poll_url(url_key, callbacks)
            time.sleep(self.interval)

    def start(self) -> None:
        """
        Start the API watcher in a separate thread.
        """
        if self._thread is not None and self._thread.is_alive():
            logger.warning("APIWatcher is already running.")
            return

        self.running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()
        logger.info(f"APIWatcher started.")

    def stop(self) -> None:
        """
        Stop the API watcher and wait for the thread to terminate.
        """
        if not self.running:
            logger.warning("APIWatcher is not running.")
            return

        self.running = False
        if self._thread is not None:
            self._thread.join()  
            logger.info(f"APIWatcher stopped.")


    @property
    def start_callbacks(self) -> List[Callable]:
        """Returns the list of start callbacks."""
        return self._start_callbacks

    def add_start_callback(self, callback: Callable) -> None:
        """Add a new start callback to be 
           triggered on file creation."""
        self._start_callbacks.append(callback)

    def remove_start_callback(self, callback: Callable) -> None:
        """Remove a start callback."""
        self._start_callbacks.remove(callback)

    @property
    def stop_callbacks(self) -> List[Callable]:
        """Returns the list of stop callbacks."""
        return self._stop_callbacks

    def add_stop_callback(self, callback: Callable) -> None:
        """Add a new stop callback to be 
           triggered on file deletion."""
        self._stop_callbacks.append(callback)

    def remove_stop_callback(self, callback: Callable) -> None:
        """Remove a stop callback."""
        self._stop_callbacks.remove(callback)
