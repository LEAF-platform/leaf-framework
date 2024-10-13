import logging
from typing import Optional

from core.metadata_manager.metadata import MetadataManager

from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)


class EventWatcher:
    def __init__(self, metadata_manager: MetadataManager, initialise_callbacks: Optional[list[str]] = None,
                 measurement_callbacks: Optional[list[str]] = None) -> None:
        if initialise_callbacks is None:
            self._initialise_callbacks = []
        elif not isinstance(initialise_callbacks,(list,set,tuple)):
            self._initialise_callbacks = [initialise_callbacks]
        else:
            self._initialise_callbacks = initialise_callbacks

        if measurement_callbacks is None:
            self._measurement_callbacks = []
        elif not isinstance(measurement_callbacks,(list,set,tuple)):
            self._measurement_callbacks = [measurement_callbacks]
        else:
            self._measurement_callbacks = measurement_callbacks
        self._metadata_manager = metadata_manager
        
    def start(self) -> None:
        equipment_data = self._metadata_manager.get_equipment_data()
        for callback in self.initialise_callbacks:
            callback(equipment_data)

    @property
    def initialise_callbacks(self)  -> list[str]:
        return self._initialise_callbacks

    def add_initialise_callback(self, callback: str) -> None:
        self._initialise_callbacks.append(callback)

    def remove_initialise_callback(self,callback: str) -> None:
        self._initialise_callbacks.remove(callback)

    @property
    def measurement_callbacks(self) -> list[str]:
        return self._measurement_callbacks

    def add_measurement_callback(self, callback: str) -> None:
        self._measurement_callbacks.append(callback)

    def remove_measurement_callback(self,callback: str) -> None:
        self._measurement_callbacks.remove(callback)

