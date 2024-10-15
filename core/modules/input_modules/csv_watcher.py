# CSVWatcher class extends FileWatcher to handle CSV file events
import logging
import os
import time
import csv

from core.modules.input_modules.file_watcher import FileWatcher
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class CSVWatcher(FileWatcher):
    """
    CSVWatcher is a specialised version of 
    FileWatcher that monitors CSV files.
    It reads the file content and passes 
    it as a list of rows to the callbacks.
    """
    def __init__(self, file_path, metadata_manager, start_callbacks=None,
                 measurement_callbacks=None, stop_callbacks=None, delimeter: str=";") -> None:
        """
        Initialise the CSVWatcher.

        Args:
            file_path: Path to the CSV file being monitored.
            metadata_manager: Manager responsible for equipment metadata.
            start_callbacks: List of callbacks to be triggered 
                             when the CSV file is created.
            measurement_callbacks: List of callbacks to be triggered 
                                   when the CSV file is modified.
            stop_callbacks: List of callbacks to be triggered 
                            when the CSV file is deleted.
            delimeter: The delimiter used in the CSV file 
                       (default is ";").
        """
        super().__init__(file_path, metadata_manager, start_callbacks,
                         measurement_callbacks, stop_callbacks)
        self._delimeter = delimeter

    def on_created(self, event):
        """
        Triggered when the CSV file is created.
        Reads the CSV content and passes it to 
        the start callbacks as a list of rows.

        Args:
            event: File system event.
        """
        fp = self._get_filepath(event)
        if fp is not None:
            self._last_created = time.time()
            with open(fp, 'r', encoding='latin-1') as file:
                reader = list(csv.reader(file, delimiter=self._delimeter))
            for callback in self._start_callbacks:
                callback(reader)

    def on_modified(self, event) -> None:
        """
        Triggered when the CSV file is modified.
        Reads the modified content and passes it 
        to the measurement callbacks as a list of rows.

        Args:
            event: File system event.
        """
        fp = self._get_filepath(event)
        if fp is None:
            return
        if self._is_last_modified():
            fp = os.path.join(self._path, self._file_name)
            with open(fp, 'r', encoding='latin-1') as file:
                reader = list(csv.reader(file, delimiter=self._delimeter))
            for callback in self._measurement_callbacks:
                callback(reader)

    def on_deleted(self, event):
        """
        Triggered when the CSV file is deleted.
        Triggers stop callbacks.
        Args:
            event: File system event.
        """
        if event.src_path.endswith(self._file_name):
            if len(self._stop_callbacks) > 0:
                for callback in self._stop_callbacks:
                    callback({})