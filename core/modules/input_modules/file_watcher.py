import logging, os, time
from typing import Any
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from core.modules.input_modules.event_watcher import EventWatcher
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class FileWatcher(FileSystemEventHandler, EventWatcher):
    """
    Watches a file for creation, modification, or deletion, triggering 
    respective callbacks on these events. Inherits from FileSystemEventHandler 
    for file system events and EventWatcher for event-based callbacks.
    """
    def __init__(self, file_path, metadata_manager, initialise_callbacks=None,
                 start_callbacks=None, measurement_callbacks=None, 
                 stop_callbacks=None) -> None:
        """
        Initialise FileWatcher.

        Args:
            file_path: Path to the file being watched.
            metadata_manager: Manages equipment metadata.
            initialise_callbacks: Callbacks for initialisation events.
            start_callbacks: Callbacks for file creation.
            measurement_callbacks: Callbacks for file modification.
            stop_callbacks: Callbacks for file deletion.
        """
        super(FileWatcher, self).__init__(metadata_manager, 
                                          initialise_callbacks=initialise_callbacks,
                                          start_callbacks=start_callbacks,
                                          measurement_callbacks=measurement_callbacks,
                                          stop_callbacks=stop_callbacks)
        self._path, self._file_name = os.path.split(file_path)
        if self._path == "": self._path = "."

        # Setup file observer
        self._observer = Observer()
        self._observer.schedule(self, self._path, recursive=False)
        
        # Debounce settings to prevent multiple event triggers
        self._last_modified = None
        self._last_created = None
        self._debounce_delay = 0.5

    def start(self) -> None:
        """
        Start monitoring the file, triggering initialisation callbacks.
        """
        if not self._observer.is_alive():
            self._observer.start()
        super().start()

    def stop(self) -> None:
        """Stop monitoring and clean up observer."""
        self._observer.stop()
        self._observer.join()

    def on_created(self, event) -> None:
        """
        Triggered on file creation, reads the file, and triggers callbacks.

        Args:
            event: File system event for file creation.
        """
        fp = self._get_filepath(event)
        if fp:
            self._last_created = time.time()
            with open(fp) as file:
                data = file.read()
            self._initiate_callbacks(self._start_callbacks, data)

    def on_modified(self, event) -> None:
        """
        Triggered on file modification, reads the file, and triggers callbacks.

        Args:
            event: File system event for file modification.
        """
        fp = self._get_filepath(event)
        if fp is None: return
        if self._is_last_modified():
            fp = os.path.join(self._path, self._file_name)
            logger.debug(f"File location {os.path.abspath(fp)}")
            with open(fp, 'r') as file:
                data = file.read()
            self._initiate_callbacks(self._measurement_callbacks, data)

    def on_deleted(self, event) -> None:
        """
        Triggered on file deletion, initiates stop callbacks with no data.

        Args:
            event: File system event for file deletion.
        """
        if event.src_path.endswith(self._file_name):
            self._initiate_callbacks(self._stop_callbacks)

    def _get_filepath(self, event) -> str:
        """
        Get file path from the event if it matches the watched file.

        Args:
            event: File system event.

        Returns:
            Full path if it matches the watched file, None otherwise.
        """
        if event.src_path.endswith(self._file_name):
            return os.path.join(self._path, self._file_name)

    def _is_last_modified(self) -> bool:
        """
        Check if the last modification is within the debounce delay.

        Returns:
            True if the event is not duplicated, False otherwise.
        """
        ct = time.time()
        if self._last_created and (ct - self._last_created) <= self._debounce_delay:
            return False
        if self._last_modified is None or (ct - self._last_modified) > self._debounce_delay:
            self._last_modified = ct
            return True
        return False
