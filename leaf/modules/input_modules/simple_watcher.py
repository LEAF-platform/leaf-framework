from typing import List, Callable

from leaf.leaf_register.metadata import MetadataManager
from leaf.modules.input_modules.polling_watcher import PollingWatcher


class SimpleWatcher(PollingWatcher):
    def __init__(self, metadata_manager: MetadataManager, interval: int, measurement_callbacks: List[Callable]) -> None:
        super().__init__(metadata_manager=metadata_manager, interval=interval, measurement_callbacks=measurement_callbacks)
        self._metadata_manager = metadata_manager
        self._measurement_callbacks = measurement_callbacks
        self._interval = interval
        self._metadata_manager = metadata_manager
        # Not used...
        self._initialise_callbacks: List[Callable] = []
        self._start_callbacks: List[Callable] = []
        self._stop_callbacks: List[Callable] = []

    def _fetch_data(self) -> dict[str, dict[str, str]]:
        # dummy data to trigger the interpreter
        return {"measurement": {"data": "data"}}