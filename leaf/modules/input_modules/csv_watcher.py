import time
import csv
from typing import Optional, List, Callable, Any
from watchdog.events import FileSystemEvent

from leaf.modules.input_modules.file_watcher import FileWatcher
from leaf.error_handler.exceptions import InputError
from leaf.error_handler.error_holder import ErrorHolder
from leaf_register.metadata import MetadataManager


class CSVWatcher(FileWatcher):
    """
    A specialised version of FileWatcher for monitoring CSV files.
    Reads the file content and dispatches it as a 
    list of rows to the specified callbacks.
    """
    def __init__(self, file_path: str, metadata_manager: MetadataManager,
                 callbacks: Optional[List[Callable]] = None, 
                 error_holder: Optional[ErrorHolder] = None, 
                 delimiter: str = ";") -> None:
        """
        Initialise the CSVWatcher.

        Args:
            file_path (str): Path to the CSV file to monitor.
            metadata_manager (MetadataManager): Metadata manager for 
                            equipment data.
            callbacks (Optional[List[Callable]]): Callbacks triggered 
                            on file events.
            error_holder (Optional[Any]): Optional error holder for 
                            capturing exceptions.
            delimiter (str): Delimiter used in the CSV file 
                            (default is ";").
        """
        super().__init__(file_path, metadata_manager, 
                         callbacks=callbacks, 
                         error_holder=error_holder)
        self._delimiter: str = delimiter

    def on_created(self, event: FileSystemEvent) -> None:
        """
        Handle CSV file creation events.
        Reads the file content and passes it to the start 
        callbacks as a list of rows.

        Args:
            event (FileSystemEvent): Event object representing 
                                     file creation.
        """
        try:
            fp = self._get_filepath(event)
            if fp is None:
                return
            self._last_created = time.time()
            with open(fp, "r", encoding="latin-1") as file:
                data = list(csv.reader(file, delimiter=self._delimiter))
        except Exception as e:
            self._file_event_exception(e, "creation")

        self._dispatch_callback(self.on_created, data)

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        Handle CSV file modification events.
        Reads the updated file content and passes it 
        to measurement callbacks as a list of rows.

        Args:
            event (FileSystemEvent): Event object representing 
                                    file modification.
        """
        try:
            fp = self._get_filepath(event)
            if fp is None:
                return
            if not self._is_last_modified():
                return
            with open(fp, 'r', encoding='latin-1') as file:
                data = list(csv.reader(file, delimiter=self._delimiter))
        except Exception as e:
            self._file_event_exception(e, "modification")
        self._dispatch_callback(self.on_modified, data)

    def _file_event_exception(self, error: Exception, 
                              event_type: str) -> None:
        """
        Handle exceptions specific to file events for CSVWatcher.

        Args:
            error (Exception): The exception encountered during 
                                file handling.
            event_type (str): The type of event during which the error 
                                occurred (e.g., creation, modification).

        Raises:
            InputError: Custom error with detailed context 
                        about the failure.
        """
        if isinstance(error, csv.Error):
            message = f"CSV parsing error in file {self._file_name} during {event_type} event: {error}"
            self._handle_exception(InputError(message))
        else:
            super()._file_event_exception(error, event_type)


