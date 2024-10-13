from typing import Optional

from core.modules.input_modules.event_watcher import EventWatcher
class APIWatcher(EventWatcher):
    def __init__(self, start_callback: Optional[str]=None,
                 measurement_callback: Optional[str]=None, stop_callback: Optional[str]=None) -> None:  # file_path,
        raise NotImplementedError()
        super().__init__(measurement_callback)

    @property
    def start_callback(self) -> str:
        return self._start_callback

    @start_callback.setter
    def start_callback(self, callback: str) -> None:
        self._start_callback = callback

    @property
    def stop_callback(self) -> str:
        return self._stop_callback

    @stop_callback.setter
    def stop_callback(self, callback: str) -> None:
        self._stop_callback = callback
        
    def start(self) -> None:
        raise NotImplementedError()
