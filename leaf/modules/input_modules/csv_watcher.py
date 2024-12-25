
from datetime import datetime
import time
import csv
from typing import Optional, List, Callable
from watchdog.events import FileSystemEvent

from leaf.modules.input_modules.file_watcher import FileWatcher
from leaf.error_handler.exceptions import InputError
from leaf_register.metadata import MetadataManager


class CSVWatcher(FileWatcher):
    """
    CSVWatcher is a specialised version of FileWatcher that monitors CSV
    files. It reads the file content and passes it as a list of rows
    to the callbacks.
    """
    def __init__(self, file_path: str, metadata_manager: MetadataManager, 
                 callbacks = None, error_holder=None,
                 delimiter: str = ";"):
        """
        Initialise CSVWatcher.

        Args:
            file_path (str): Path to the CSV file being monitored.
            metadata_manager (MetadataManager): Manager responsible 
                             for equipment metadata.
            start_callbacks (Optional[List[Callable]]): List of callbacks 
                            to be triggered when the CSV file is created.
            measurement_callbacks (Optional[List[Callable]]): List of 
                                  callbacks to be triggered when the 
                                  CSV file is modified.
            stop_callbacks (Optional[List[Callable]]): List of callbacks 
                            to be triggered when the CSV file is deleted.
            delimiter (str): The delimiter used in the CSV file 
                             (default is ";").
        """
        super().__init__(file_path, metadata_manager,
                         callbacks=callbacks,
                         error_holder=error_holder)
        self._delimiter = delimiter

    def on_created(self, event: FileSystemEvent) -> None:
        """
        Triggered on CSV file creation. Reads content and passes it 
        to start callbacks as a list of rows.

        Args:
            event (FileSystemEvent): File system event.
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
        
        return self._dispatch_callback(self.on_created,data)

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        Triggered on CSV file modification. Reads modified content and 
        passes it to measurement callbacks as a list of rows.

        Args:
            event (FileSystemEvent): File system event.
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

        return self._dispatch_callback(self.on_modified,data)

    def _file_event_exception(self, error: Exception, event_type: str) -> None:
        """
        Extends file-related exception 
        handling with CSV-specific errors.
        
        Args:
            error (Exception): The exception to handle.
            event_type (str): Type of the event

        Raises:
            InputError: Custom error encapsulating the specific issue.
        """
        if isinstance(error, csv.Error):
            message = f"CSV parsing error in file {self._file_name} during {event_type} event: {error}"
            self._handle_exception(InputError(message))
        else:
            super()._file_event_exception(error, event_type)
