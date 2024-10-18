import logging
import os
import time
from threading import Event
from abc import ABC, abstractmethod
from core.metadata_manager.metadata import MetadataManager

# Get the current directory path and set the path to the equipment adapter metadata file
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'equipment_adapter.json')

# Import custom logger from core module
from core.modules.logger_modules.logger_utils import get_logger

# Initialize the logger for this module with debug level logging
logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class EquipmentAdapter(ABC):
    """
    Base class for all equipment adapters. Ensures all derived classes have 
    the required composite modules and implements a start and stop function.
    """
    def __init__(self, instance_data, watcher, process_adapters, interpreter, 
                 metadata_manager=None):
        """
        Initialize the EquipmentAdapter instance.
        Args:
            instance_data: Data related to the equipment instance.
            watcher: An object that watches or monitors events or data.
            process_adapters: A list or a single instance of ProcessaAdapters.
            interpreter: An interpreter object to process the data.
            metadata_manager: Optional metadata manager instance (defaults to new MetadataManager if None).
        """
        if not isinstance(process_adapters, (list, tuple, set)):
            process_adapters = [process_adapters]
        
        logger.debug(f"Process adapters {process_adapters}")
        self._processes = process_adapters
        logger.debug(f"Processes {self._processes}")

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
        logger.debug(f"Metadata manager {self._metadata_manager}")

    def start(self):
        """
        Start the equipment adapter process.

        Starts the watcher and loops.
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

    def stop(self) -> None:
        """
        Stop the equipment adapter process.

        Stops the watcher and flushes all output channels.
        """
        # Needs reworking really.
        for process in self._processes:
            for phase in process._phases:
                # Flush all retained mqtt topics.
                phase._output.flush(self._metadata_manager.details())
                phase._output.flush(self._metadata_manager.running())
                phase._output.flush(self._metadata_manager.experiment.start())
                phase._output.flush(self._metadata_manager.experiment.start())
        self._stop_event.set()
        self._watcher.stop()

class AbstractInterpreter(ABC):
    """
    Abstract base class for interpreters.

    One interpreter is needed for each EquipmentAdapter class.
    """
    def __init__(self):
        """
        Initialize the abstract interpreter with predefined keys for 
        measurement and metadate outputs.
        """
        self.id = 'undefined'
        self.TIMESTAMP_KEY = "timestamp"
        self.EXPERIMENT_ID_KEY = "experiment_id"
        self.MEASUREMENT_HEADING_KEY = "measurement_types"
        
    @abstractmethod
    def metadata(self, data):
        """
        Abstract method to process metadata.
        
        Args:
            data: The metadata from the InputModule.
        """
        pass

    @abstractmethod
    def measurement(self, data):
        """
        Abstract method to process measurement data.
        
        Args:
            data: The measurement from the InputModule.
        """
        pass

    @abstractmethod    
    def simulate(self):
        """
        Abstract method to simulate a finite 
        run of equipment using existing data.
        """
        pass
