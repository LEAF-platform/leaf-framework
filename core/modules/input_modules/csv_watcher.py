import logging, os, time, csv
from core.modules.input_modules.file_watcher import FileWatcher
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class CSVWatcher(FileWatcher):
    """
    Specialised FileWatcher for monitoring CSV files. Reads file content 
    and passes it as a list of rows to the specified callbacks.
    """
    def __init__(self, file_path, metadata_manager, initialise_callbacks=None,
                 start_callbacks=None, measurement_callbacks=None, 
                 stop_callbacks=None, delimiter: str = ";") -> None:
        """
        Initialise CSVWatcher.

        Args:
            file_path: Path to the CSV file to monitor.
            metadata_manager: Manages metadata for equipment.
            initialise_callbacks: Callbacks for initialisation events.
            start_callbacks: Callbacks triggered on file creation.
            measurement_callbacks: Callbacks triggered on file modification.
            stop_callbacks: Callbacks triggered on file deletion.
            delimiter: The delimiter used in the CSV file (default ";").
        """
        super().__init__(file_path, metadata_manager, 
                         initialise_callbacks=initialise_callbacks,
                         start_callbacks=start_callbacks, 
                         measurement_callbacks=measurement_callbacks, 
                         stop_callbacks=stop_callbacks)
        self._delimiter = delimiter

    def on_created(self, event) -> None:
        """
        Triggered on CSV file creation. Reads content and passes it 
        to start callbacks as a list of rows.

        Args:
            event: File system event related to file creation.
        """
        fp = self._get_filepath(event)
        if fp:
            self._last_created = time.time()
            with open(fp, "r", encoding="latin-1") as file:
                reader = list(csv.reader(file, delimiter=self._delimiter))
            self._initiate_callbacks(self._start_callbacks, reader)

    def on_modified(self, event) -> None:
        """
        Triggered on CSV file modification. Reads modified content and 
        passes it to measurement callbacks as a list of rows.

        Args:
            event: File system event related to file modification.
        """
        logger.debug(f"CSVWatcher: File modified: {event.src_path}")
        fp = self._get_filepath(event)
        if fp is None:
            return
        if self._is_last_modified():
            fp = os.path.join(self._path, self._file_name)
            with open(fp, 'r', encoding='latin-1') as file:
                reader = list(csv.reader(file, delimiter=self._delimiter))
            self._initiate_callbacks(self._measurement_callbacks, reader)

    def on_deleted(self, event):
        """
        Triggered on CSV file deletion. Triggers stop callbacks.

        Args:
            event: File system event related to file deletion.
        """
        if event.src_path.endswith(self._file_name):
            if len(self._stop_callbacks) > 0:
                self._initiate_callbacks(self._stop_callbacks)
