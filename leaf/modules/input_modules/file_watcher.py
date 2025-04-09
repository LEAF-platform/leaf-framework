import os
import time
import errno
import logging
from datetime import datetime
from typing import Optional
from typing import Callable
from typing import List

from watchdog.events import FileSystemEvent
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.exceptions import InputError
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf.utility.logger.logger_utils import get_logger

logger = get_logger(__name__, log_file="input_module.log", 
                    log_level=logging.DEBUG)


class FileWatcher(FileSystemEventHandler, EventWatcher):
    """
    Monitors a specific file for creation, modification, and deletion events.
    Utilises the `watchdog` library for event monitoring and triggers callbacks
    for each event type.
    """

    def __init__(
        self,
        path: str,
        metadata_manager: MetadataManager,
        callbacks: Optional[List[Callable[[str, str], None]]] = None,
        error_holder: Optional[ErrorHolder] = None,
    ) -> None:
        """
        Initialise FileWatcher.

        Args:
            path (str): Path to the file or directory to monitor.
            metadata_manager (MetadataManager): Metadata manager for associated data.
            callbacks (Optional[List[Callable]]): Callbacks for file events.
            error_holder (Optional[ErrorHolder]): Optional error holder for capturing exceptions.

        Raises:
            AdapterBuildError: Raised if the provided file path is invalid.
        """
        super().__init__(
            metadata_manager=metadata_manager,
            callbacks=callbacks,
            error_holder=error_holder,
        )

        logger.debug(f"Initialising FileWatcher with file path {path}")

        try:
            if os.path.isdir(path):
                self._path = path
                self._file_name = None
            else:
                self._path, self._file_name = os.path.split(path)
                if not self._path:
                    self._path = "."
        except TypeError:
            raise AdapterBuildError(f"{path} is not a valid path for FileWatcher.")

        self._observer = Observer()
        self._observing = False
        self._observer.schedule(self, self._path, recursive=False)

        self._last_created: Optional[float] = None
        self._last_modified: Optional[float] = None
        self._debounce_delay: float = 0.75

        self._term_map = {
            self.on_created: metadata_manager.experiment.start,
            self.on_modified: metadata_manager.experiment.measurement,
            self.on_deleted: metadata_manager.experiment.stop,
        }

    def start(self) -> None:
        """
        Begin observing the file path for events.
        Ensures a single observer instance is active.
        """
        if self._observing:
            logger.warning("FileWatcher is already running.")
            return

        os.makedirs(self._path, exist_ok=True)
        try:
            self._observer = Observer()
            self._observer.schedule(self, self._path, recursive=False)
            if not self._observer.is_alive():
                self._observer.start()
            super().start()
            self._observing = True
            self._last_created = None
            self._last_modified = None
            logger.info("FileWatcher started.")
        except OSError as e:
            self._handle_exception(self._create_input_error(e))
        except Exception as ex:
            self._handle_exception(InputError(f"Error starting observer: {ex}"))

    def stop(self) -> None:
        """
        Stop observing the file for events.
        Terminates the observer thread safely.
        """
        if not self._observing:
            logger.warning("FileWatcher is not running.")
            return

        self._observer.stop()
        self._observer.join()
        super().stop()
        self._observing = False
        logger.info("FileWatcher stopped.")

    def on_created(self, event: FileSystemEvent) -> None:
        """
        Handle file creation events and trigger start callbacks.

        Args:
            event (FileSystemEvent): Event object indicating a file creation.
        """
        data = {}
        try:
            fp = self._get_filepath(event)
            if fp is None:
                return
            self._last_created = time.time()
            with open(fp, "r") as file:
                data = file.read()
        except Exception as e:
            self._file_event_exception(e, "creation")
        self._dispatch_callback(self._term_map[self.on_created], data)

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        Handle file modification events and trigger measurement callbacks.

        Args:
            event (FileSystemEvent): Event object indicating a file modification.
        """
        try:
            fp = self._get_filepath(event)
            if fp is None or not self._is_last_modified():
                return
            with open(fp, "r") as file:
                data = file.read()
        except Exception as e:
            self._file_event_exception(e, "modification")
        self._dispatch_callback(self._term_map[self.on_modified], data)

    def on_deleted(self, event: FileSystemEvent) -> None:
        """
        Handle file deletion events and trigger stop callbacks.

        Args:
            event (FileSystemEvent): Event object indicating a file deletion.
        """
        if self._file_name is None or event.src_path.endswith(self._file_name):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._dispatch_callback(self._term_map[self.on_deleted], 
                                    timestamp)

    def _get_filepath(self, event: FileSystemEvent) -> Optional[str]:
        """
        Retrieve the full file path for the event if it matches the watched file.

        Args:
            event (FileSystemEvent): Event object containing file information.

        Returns:
            Optional[str]: Full file path if it matches the watched file, otherwise None.
        """
        if self._file_name is None:
            return event.src_path
        elif event.src_path.endswith(self._file_name):
            return os.path.join(self._path, self._file_name)
        return None

    def _is_last_modified(self) -> bool:
        """
        Determine if the file modification is outside the debounce delay.

        Returns:
            bool: True if the modification event is outside the debounce period, False otherwise.
        """
        current_time = time.time()
        if self._last_created and (current_time - self._last_created) <= self._debounce_delay:
            return False
        if self._last_modified is None or (current_time - self._last_modified) > self._debounce_delay:
            self._last_modified = current_time
            return True
        return False

    def _file_event_exception(self, error: Exception, event_type: str) -> None:
        """
        Log and handle exceptions during file events.

        Args:
            error (Exception): Exception encountered during event handling.
            event_type (str): Type of event that triggered the exception.
        """
        file_name = self._file_name or "unspecified"
        if isinstance(error, FileNotFoundError):
            message = f"File not found during {event_type} event: {file_name}"
        elif isinstance(error, PermissionError):
            message = f"Permission denied when accessing file during {event_type} event: {file_name}"
        elif isinstance(error, IOError):
            message = f"I/O error during {event_type} event in file {file_name}: {error}"
        elif isinstance(error, UnicodeDecodeError):
            message = f"Encoding error while reading file {file_name} during {event_type} event: {error}"
        else:
            message = f"Error during {event_type} event in file {file_name}: {error}"
        self._handle_exception(InputError(message))


    def _create_input_error(self, e: OSError) -> InputError:
        """
        Map OS errors to custom InputError messages.

        Args:
            e (OSError): Operating system error encountered.

        Returns:
            InputError: Custom error based on the OS error code.
        """
        if e.errno == errno.EACCES:
            return InputError(f"Permission denied: Unable to access {self._path}")
        elif e.errno == errno.ENOSPC:
            return InputError("Inotify watch limit reached. Cannot add more watches.")
        elif e.errno == errno.ENOENT:
            return InputError(f"Watch file does not exist: {self._path}")
        return InputError(f"Unexpected OS error: {e}")
