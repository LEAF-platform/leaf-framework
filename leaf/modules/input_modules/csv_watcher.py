import csv
import time

from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from watchdog.events import FileSystemEvent

from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import InputError
from leaf.modules.input_modules.file_watcher import FileWatcher


EventCallback = Callable[[str, Any], None]


class CSVWatcher(FileWatcher):
    """
    A specialised FileWatcher for monitoring CSV files.
    Reads and parses the CSV file content on create/modify events
    and dispatches it to registered callbacks.
    """

    def __init__(
        self,
        file_path: str,
        metadata_manager: MetadataManager,
        callbacks: Optional[List[EventCallback]] = None,
        error_holder: Optional[ErrorHolder] = None,
        delimiter: str = ";"
    ) -> None:
        """
        Initialise the CSVWatcher.

        Args:
            file_path (str): Path to the CSV file to monitor.
            metadata_manager (MetadataManager): Metadata manager for experiment and instance metadata.
            callbacks (Optional[List[EventCallback]]): Callback functions triggered on file events.
            error_holder (Optional[ErrorHolder]): Optional error handler to capture and manage exceptions.
            delimiter (str): Delimiter used in the CSV file (default is ";").
        """
        super().__init__(
            file_path,
            metadata_manager,
            callbacks=callbacks,
            error_holder=error_holder
        )
        self._delimiter: str = delimiter

    def on_created(self, event: FileSystemEvent) -> None:
        """
        Handle file creation events by reading and parsing CSV content.

        Args:
            event (FileSystemEvent): The event indicating file creation.
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
        else:
            self._dispatch_callback(self._term_map[self.on_created], data)
            

    def on_modified(self, event: FileSystemEvent) -> None:
        """
        Handle file modification events by reading and parsing updated CSV content.

        Args:
            event (FileSystemEvent): The event indicating file modification.
        """
        try:
            fp = self._get_filepath(event)
            if fp is None or not self._is_last_modified():
                return
            with open(fp, "r", encoding="latin-1") as file:
                data = list(csv.reader(file, delimiter=self._delimiter))
        except Exception as e:
            self._file_event_exception(e, "modification")
        else:
            self._dispatch_callback(self._term_map[self.on_modified], data)

    def _file_event_exception(self, error: Exception, event_type: str) -> None:
        """
        Handle and log exceptions that occur during file events.

        Args:
            error (Exception): The caught exception.
            event_type (str): The type of file event ("creation", "modification", etc.).
        """
        if isinstance(error, csv.Error):
            msg = f"CSV parsing error in file {self._file_name or 'unknown'} during {event_type}: {error}"
            self._handle_exception(InputError(msg))
        else:
            super()._file_event_exception(error, event_type)
