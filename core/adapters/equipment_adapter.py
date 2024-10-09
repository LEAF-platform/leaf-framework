import os
import time
from threading import Event
from abc import ABC, abstractmethod
from core.metadata_manager.metadata import MetadataManager

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'equipment_adapter.json')

class EquipmentAdapter:
    def __init__(self,instance_data,watcher,process_adapters,interpreter,
                 metadata_manager=None):
        if not isinstance(process_adapters,(list,tuple,set)):
            process_adapters = [process_adapters]
        self._processes = process_adapters
        [p.set_interpreter(interpreter) for p in self._processes]
        self._interpreter = interpreter
        self._watcher = watcher
        self._stop_event = Event()
        if metadata_manager is None:
            self._metadata_manager = MetadataManager()
        else:
            self._metadata_manager = metadata_manager
        self._metadata_manager.load_from_file(metadata_fn)
        self._metadata_manager.add_equipment_data(instance_data)

    def start(self):
        """
        Start all watchers and keep the proxy running until interrupted.
        """
        self._watcher.start()
        try:
            while not self._stop_event.is_set():
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass
        finally:
            self._watcher.stop()
            self.stop()

    def stop(self):
        """
        Signal the proxy to stop.
        """
        # Needs reworking really.
        for process in self._processes:
            for phase in process._phases:
                phase._output.flush(self._metadata_manager.details())
                phase._output.flush(self._metadata_manager.running())
                phase._output.flush(self._metadata_manager.experiment.start())
                phase._output.flush(self._metadata_manager.experiment.start())
        self._stop_event.set()
        self._watcher.stop()

class AbstractInterpreter(ABC):
    def __init__(self):
        self.id = None
        # could consider if to capture these more formally.
        self.TARGET_PARAMS_KEY = "target_parameters"
        self.INITIAL_PARAMS_KEY = "initial_parameters"
        self.SENSORS_KEY = "sensors"
        self.TIMESTAMP_KEY = "timestamp"
        self.EXPERIMENT_ID_KEY = "experiment_id"
        self.UPDATE_KEY = "measurements"
    
    @abstractmethod
    def metadata(self,data):
        pass

    @abstractmethod
    def measurement(self,data):
        pass

    @abstractmethod    
    def simulate(self):
        pass

    