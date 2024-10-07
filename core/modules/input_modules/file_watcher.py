import os
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from core.modules.input_modules.event_watcher import EventWatcher

class FileWatcher(FileSystemEventHandler,EventWatcher):
    def __init__(self, file_path, start_callbacks=None,
                 measurement_callbacks=None, stop_callbacks=None):
        super(FileWatcher, self).__init__(measurement_callbacks=measurement_callbacks)  
        
        self._path, self._file_name = os.path.split(file_path)
        if self._path == "":
            self._path = "."
        self._observer = Observer()
        self._observer.schedule(self, self._path, recursive=False)

        if start_callbacks is None:
            self._start_callbacks = []
        elif not isinstance(start_callbacks,(list,set,tuple)):
            self._start_callbacks = [start_callbacks]
        else:
            self._start_callbacks = start_callbacks

        if stop_callbacks is None:
            self._stop_callbacks = []
        elif not isinstance(stop_callbacks,(list,set,tuple)):
            self._stop_callbacks = [stop_callbacks]
        else:
            self._stop_callbacks = stop_callbacks
        # Some filesystems such as ext4 will have two 
        # events on filechange, one to change content and one metadata.
        # Use debouncing to stop two callbacks on one change.
        self._last_modified = None
        self._last_created = None
        self._debounce_delay = 0.5


    @property
    def start_callbacks(self):
        return self._start_callbacks

    def add_start_callback(self, callback):
        self._start_callbacks.append(callback)

    def remove_start_callback(self,callback):
        self._start_callbacks.remove(callback)

    @property
    def stop_callbacks(self):
        return self._start_callbacks

    def add_stop_callback(self, callback):
        self._stop_callbacks.append(callback)

    def remove_stop_callback(self,callback):
        self._stop_callbacks.remove(callback)
        
    def start(self):
        if not self._observer.is_alive():
            self._observer.start()
        super().start()

    def stop(self):
        self._observer.stop()
        self._observer.join()

    def on_created(self, event):
        fp = self._get_filepath(event)
        if fp is not None:
            self._last_created = time.time()
            with open(fp) as file:
                data = file.read()
            for callback in self._start_callbacks:
                callback(data)

    def on_modified(self, event):
        fp = self._get_filepath(event)
        if fp is None:
            return
        if self._is_last_modified():
            fp = os.path.join(self._path,self._file_name)
            with open(fp, 'r') as file:
                data = file.read()
            for callback in self._measurement_callbacks:
                callback(data)

    def on_deleted(self, event):
        if event.src_path.endswith(self._file_name):
            if len(self._stop_callbacks) > 0:
                for callback in self._stop_callbacks:
                    callback({})

    def _get_filepath(self,event):
        if event.src_path.endswith(self._file_name):
            return os.path.join(self._path,self._file_name)

    def _is_last_modified(self):
        ct = time.time()
        if (self._last_created and 
            (ct - self._last_created) <= self._debounce_delay):
            return False
        if (self._last_modified is None or
            (ct - self._last_modified) > self._debounce_delay):
            self._last_modified = ct
        return True
    
