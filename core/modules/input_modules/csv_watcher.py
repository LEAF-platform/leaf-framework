import os
import time
import csv

from core.modules.input_modules.file_watcher import FileWatcher

class CSVWatcher(FileWatcher):
    def __init__(self, file_path, start_callbacks=None,
                 measurement_callbacks=None, stop_callbacks=None,delimeter=";"):
        super().__init__(file_path,start_callbacks,
                         measurement_callbacks,stop_callbacks)
        self._delimeter=delimeter

    def on_created(self, event):
        fp = self._get_filepath(event)
        if fp is not None:
            self._last_created = time.time()
            with open(fp, 'r', encoding='latin-1') as file:
                reader = list(csv.reader(file, delimiter=self._delimeter))
            for callback in self._start_callbacks:
                callback(reader)

    def on_modified(self, event):
        fp = self._get_filepath(event)
        if fp is None:
            return
        if self._is_last_modified():
            fp = os.path.join(self._path,self._file_name)
            with open(fp, 'r', encoding='latin-1') as file:
                reader = list(csv.reader(file, delimiter=self._delimeter))
            for callback in self._measurement_callbacks:
                callback(reader)


    def on_deleted(self, event):
        if event.src_path.endswith(self._file_name):
            if len(self._stop_callbacks) > 0:
                for callback in self._stop_callbacks:
                    callback({})