import os

from core.modules.input_modules.event_watcher import EventWatcher

class DBWatcher(EventWatcher):
    def __init__(self, file_path, start_callback=None,
                 measurement_callback=None, stop_callback=None):
        raise NotImplementedError()
        super().__init__(instance_manifest)

    @property
    def start_callback(self):
        return self._start_callback

    @start_callback.setter
    def start_callback(self, callback):
        self._start_callback = callback

    @property
    def stop_callback(self):
        return self._stop_callback

    @stop_callback.setter
    def stop_callback(self, callback):
        self._stop_callback = callback
        
    def start(self):
        raise NotImplementedError()
