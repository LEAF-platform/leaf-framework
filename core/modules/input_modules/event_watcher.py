
from core.metadata_manager.metadata import metadata_manager
class EventWatcher:
    def __init__(self,initialise_callbacks=None,
                 measurement_callbacks=None):
        super().__init__()
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

    def start(self):
        equipment_data = metadata_manager.get_equipment_data()
        for callback in self.initialise_callbacks:
            callback(equipment_data)

    @property
    def initialise_callbacks(self):
        return self._initialise_callbacks

    def add_initialise_callback(self, callback):
        self._initialise_callbacks.append(callback)

    def remove_initialise_callback(self,callback):
        self._initialise_callbacks.remove(callback)

    @property
    def measurement_callbacks(self):
        return self._measurement_callbacks

    def add_measurement_callback(self, callback):
        self._measurement_callbacks.append(callback)

    def remove_measurement_callback(self,callback):
        self._measurement_callbacks.remove(callback)

