import os
import time
import logging
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from core.modules.input_modules.event_watcher import EventWatcher
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class FileWatcher(FileSystemEventHandler, EventWatcher):
    """
    Monitors changes to a specific file, such as when a 
    file is created, modified, or deleted. It triggers appropriate 
    callbacks (to PhaseModules) when these events occur.

    Inherits from both FileSystemEventHandler (to handle file system events) 
    and EventWatcher (to trigger callbacks based on those events).
    """
    def __init__(self, file_path, metadata_manager, start_callbacks=None,
                 measurement_callbacks=None, stop_callbacks=None):
        """
        Initialise FileWatcher.

        Args:
            file_path: Path to the file being watched.
            metadata_manager: Manager responsible for equipment metadata.
            start_callbacks: List of callbacks to be triggered 
                             when the file is created.
            measurement_callbacks: List of callbacks to be triggered 
                                   when the file is modified.
            stop_callbacks: List of callbacks to be triggered 
                            when the file is deleted.
        """
        super(FileWatcher, self).__init__(metadata_manager,
                                          measurement_callbacks=measurement_callbacks)
        logger.debug(f"Initialising FileWatcher with file path {file_path}")
        self._path, self._file_name = os.path.split(file_path)
        if self._path == "":
            self._path = "."

        # Create an observer to watch for file system changes
        self._observer = Observer()
          # Only watch for changes in the specified directory
        self._observer.schedule(self, self._path, recursive=False)

        if start_callbacks is None:
            self._start_callbacks = []
        elif not isinstance(start_callbacks, (list, set, tuple)):
            self._start_callbacks = [start_callbacks]
        else:
            self._start_callbacks = start_callbacks
        
        if stop_callbacks is None:
            self._stop_callbacks = []
        elif not isinstance(stop_callbacks, (list, set, tuple)):
            self._stop_callbacks = [stop_callbacks]
        else:
            self._stop_callbacks = stop_callbacks

        # Prevent multiple events from being 
        # fired on the same change (debouncing)
        self._last_modified = None
        self._last_created = None
        self._debounce_delay = 0.5

    @property
    def start_callbacks(self):
        """Returns the list of start callbacks."""
        return self._start_callbacks

    def add_start_callback(self, callback) -> None:
        """Add a new start callback to be 
           triggered on file creation."""
        self._start_callbacks.append(callback)

    def remove_start_callback(self, callback):
        """Remove a start callback."""
        self._start_callbacks.remove(callback)

    @property
    def stop_callbacks(self):
        """Returns the list of stop callbacks."""
        return self._stop_callbacks

    def add_stop_callback(self, callback) -> None:
        """Add a new stop callback to be 
           triggered on file deletion."""
        self._stop_callbacks.append(callback)

    def remove_stop_callback(self, callback):
        """Remove a stop callback."""
        self._stop_callbacks.remove(callback)

    def start(self):
        """
        Start the observer to begin monitoring the file.
        Also triggers any initialisation that EventWatcher requires.
        """
        if not self._observer.is_alive():
            self._observer.start()
        super().start()

    def stop(self):
        """Stop the observer and clean up."""
        self._observer.stop()
        self._observer.join()

    def on_created(self, event):
        """
        Triggered when the file is created.
        Reads the file and callsback with this data.

        Args:
            event: File system event.
        """
        fp = self._get_filepath(event)
        if fp is not None:
            self._last_created = time.time()
            with open(fp) as file:
                data = file.read()
            for callback in self._start_callbacks:
                callback(data)

    def on_modified(self, event):
        """
        Triggered when the file is modified.
        Reads the file and callsback with this data.

        Args:
            event: File system event.
        """
        fp = self._get_filepath(event)
        if fp is None:
            return
        # Ensure modification is not a duplicate event (debouncing)
        if self._is_last_modified():
            fp = os.path.join(self._path, self._file_name)
            logger.debug(f"File location {os.path.abspath(fp)}")
            with open(fp, 'r') as file:
                data = file.read()
            for callback in self._measurement_callbacks:
                callback(data)

    def on_deleted(self, event):
        """
        Triggered when the file is deleted.
        Callsback with no data.

        Args:
            event: File system event.
        """
        if event.src_path.endswith(self._file_name):
            if len(self._stop_callbacks) > 0:
                for callback in self._stop_callbacks:
                    callback({})

    def _get_filepath(self, event):
        """
        Get the full file path from the 
        event if it matches the watched file.

        Args:
            event: File system event.

        Returns:
            Full file path if it matches 
            the watched file, None otherwise.
        """
        if event.src_path.endswith(self._file_name):
            return os.path.join(self._path, self._file_name)

    def _is_last_modified(self):
        """
        Check if the last modification occurred 
        within the debounce delay period.

        Returns:
            True if the event is valid, 
            False if it's a duplicate event.
        """
        ct = time.time()
        # If file was just created, ignore within debounce delay
        if self._last_created and (ct - self._last_created) <= self._debounce_delay:
            return False
        if self._last_modified is None or (ct - self._last_modified) > self._debounce_delay:
            self._last_modified = ct
        return True
    

