import os
import time
from datetime import datetime
import logging
import errno
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from typing import Optional, List, Callable
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.error_handler.exceptions import AdapterBuildError, InputError
from leaf.metadata_manager.metadata import MetadataManager
logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class FileWatcher(FileSystemEventHandler, EventWatcher):
    """
    Watches a specified file for creation, modification, 
    and deletion events, triggering callbacks based on events.
    
    Uses the `watchdog` library to monitor the file system and manage 
    file event handling with debounce and error management. 
    Ensures callbacks are executed only once within a 
    specified debounce period.
    """

    def __init__(self, 
                 file_path: str, 
                 metadata_manager: MetadataManager, 
                 start_callbacks: Optional[List[Callable]] = None, 
                 measurement_callbacks: Optional[List[Callable]] = None, 
                 stop_callbacks: Optional[List[Callable]] = None,
                 last_line: bool=False):
        """
        Initialise the FileWatcher with the specified 
        file path and callbacks.

        Args:
            file_path (str): Path to the file being watched.
            metadata_manager (MetadataManager): The metadata manager 
                                                for managing metadata 
                                                associated with the 
                                                watched file.
            start_callbacks (Optional[List[Callable]]): Callbacks 
                            triggered on file creation events.
            measurement_callbacks (Optional[List[Callable]]): Callbacks 
                                  triggered on file modification events.
            stop_callbacks (Optional[List[Callable]]): Callbacks triggered
                            on file deletion events.
            last_line (Optional bool): Flag for returning only the 
                      last time when the file is modified.
        Raises:
            AdapterBuildError: If the file path is invalid.
        """
        super(FileWatcher, self).__init__(metadata_manager, 
                                          measurement_callbacks=measurement_callbacks)
        logger.debug(f"Initialising FileWatcher with file path {file_path}")
        
        try:
            self._path, self._file_name = os.path.split(file_path)
        except TypeError:
            raise AdapterBuildError(f'{file_path} is not a valid path for FileWatcher.')
        
        if self._path == "":
            self._path = ""

        # Observer attributes
        self._observer = Observer()
        self._observing = False
        self._observer.schedule(self, self._path, recursive=False)
        
        # Callbacks for events
        self._start_callbacks = self._cast_callbacks(start_callbacks)
        self._stop_callbacks = self._cast_callbacks(stop_callbacks)
        
        # Debounce control attributes
        self._last_modified: Optional[float] = None
        self._last_created: Optional[float] = None
        self._debounce_delay: float = 0.75

        self._last_line = last_line

    @property
    def start_callbacks(self) -> List[Callable]:
        """List[Callable]: Callbacks triggered on file creation."""
        return self._start_callbacks

    def add_start_callback(self, callback: Callable) -> None:
        """Add a new callback to be triggered on file creation."""
        self._start_callbacks.append(callback)

    def remove_start_callback(self, callback: Callable) -> None:
        """Remove a callback from the start callbacks list."""
        self._start_callbacks.remove(callback)

    @property
    def stop_callbacks(self) -> List[Callable]:
        """List[Callable]: Callbacks triggered on file deletion."""
        return self._stop_callbacks

    def add_stop_callback(self, callback: Callable) -> None:
        """Add a new callback to be triggered on file deletion."""
        self._stop_callbacks.append(callback)

    def remove_stop_callback(self, callback: Callable) -> None:
        """Remove a callback from the stop callbacks list."""
        self._stop_callbacks.remove(callback)

    def start(self) -> None:
        """
        Begin observing the specified file path for events. 
        
        Starts the observer and ensures only one observer is running 
        at a time. Handles and logs any errors encountered during 
        the start process.
        """
        if not self._observing:
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
                exception = self._create_input_error(e)
                self._handle_exception(exception)
            except Exception as ex:
                ex_str = f"Error starting observer: {ex}"
                self._handle_exception(InputError(ex_str))
        else:
            logger.warning("FileWatcher is already running.")

    def stop(self) -> None:
        """
        Stop observing the file for events.
        
        Stops and joins the observer thread, 
        marking the watcher as inactive.
        """
        if self._observing:
            self._observer.stop()
            self._observer.join()
            super().stop()
            self._observing = False
            logger.info("FileWatcher stopped.")
        else:
            logger.warning("FileWatcher is not running.")

    def on_created(self, event) -> None:
        """
        Reads the conent of new files when triggered and start callbacks.

        Args:
            event: The event object from watchdog 
                   indicating a file creation.
        """
        data = {}
        try:
            fp = self._get_filepath(event)
            if fp is None:
                return
            self._last_created = time.time()
            with open(fp, 'r') as file:
                data = file.read()
        except Exception as e:
            self._file_event_exception(e, "creation")
        self._initiate_callbacks(self._start_callbacks, data)

    def on_modified(self, event) -> None:
        """
        Handle file modification events and measurement callbacks. 
        Uses debounce to avoid multiple triggers in quick succession.
        
        Args:
            event: The event object from watchdog 
                   indicating a file modification.
        """
        try:
            fp = self._get_filepath(event)
            if fp is None:
                return
            if not self._is_last_modified():
                return
            with open(fp, 'r') as file:
                if self._last_line:
                    data = [file.readlines()[-1]]
                else:
                    data = file.read()
        except Exception as e:
            self._file_event_exception(e, "modification")
        self._initiate_callbacks(self._measurement_callbacks, data)

    def on_deleted(self, event) -> None:
        """
        Handle file deletion events by triggering the stop callbacks.

        Args:
            event: The event object from watchdog indicating a file deletion.
        """
        if event.src_path.endswith(self._file_name):
            if len(self._stop_callbacks) > 0:
                data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._initiate_callbacks(self._stop_callbacks, data)

    def _get_filepath(self, event) -> Optional[str]:
        """
        Get the full file path for the event if 
        it matches the watched file.

        Args:
            event: The event object containing file information.
        
        Returns:
            Optional[str]: The full file path if it matches the 
                           watched file, otherwise None.
        """
        if event.src_path.endswith(self._file_name):
            return os.path.join(self._path, self._file_name)
        return None

    def _is_last_modified(self) -> bool:
        """
        Check if the file modification event is outside the 
        debounce delay.
        
        Returns:
            bool: True if the event is outside the debounce period, 
                  False otherwise.
        """
        ct = time.time()
        if self._last_created and (ct - self._last_created) <= self._debounce_delay:
            return False
        if self._last_modified is None or (ct - self._last_modified) > self._debounce_delay:
            self._last_modified = ct
        return True

    def _file_event_exception(self, error: Exception, event_type: str) -> None:
        """
        Log an appropriate error message and create an 
        InputError based on the event type and exception type.
        
        Args:
            error (Exception): The exception encountered during the event.
            event_type (str): The type of event ('creation', 
                              'modification', 'deletion').
        """
        file_name = self._file_name
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
        Map OSError codes to custom InputError messages.

        Args:
            e (OSError): The operating system error encountered.
        
        Returns:
            InputError: Custom error message based on the OSError code.
        """
        if e.errno == errno.EACCES:
            return InputError(f'Permission denied: Unable to access {self._path}')
        elif e.errno == errno.ENOSPC:
            return InputError('Inotify watch limit reached. Cannot add more watches')
        elif e.errno == errno.ENOENT:
            return InputError(f'Watch file does not exist: {self._path}')
        return InputError(f'Unexpected OS error: {e}')
