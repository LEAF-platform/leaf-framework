from typing import Optional, Callable, List
from leaf_register.metadata import MetadataManager
from abc import ABC
from abc import abstractmethod

class EventWatcher(ABC):
    """
    Aims to monitor and extract specific information from the equipment. 
    It is designed to detect and handle events, such as when equipment 
    provides measurements by writing to a file or any other 
    observable event. 
    """
    def __init__(self, metadata_manager: MetadataManager, 
                 initialise_callbacks: Optional[list[Callable]] = None,
                 measurement_callbacks: Optional[list[Callable]] = None,
                 error_holder=None) -> None:
        """
        Initialise the EventWatcher instance.

        Args:
            metadata_manager: An instance of MetadataManager 
                              to manage equipment data.
            initialise_callbacks: An optional list of callback 
                                  functions to be executed 
                                  during the initialisation 
                                  of the adapter.
            measurement_callbacks: An optional list of callback 
                                    functions to be executed when 
                                    a measurement event occurs.
        """
        
        self._initialise_callbacks = self._cast_callbacks(initialise_callbacks)
        self._measurement_callbacks = self._cast_callbacks(measurement_callbacks)        
        self._metadata_manager = metadata_manager
        self._error_holder = error_holder
        self._running = False

    @abstractmethod
    def start(self) -> None:
        """
        Start the EventWatcher and trigger the initialise callbacks.
        """
        equipment_data = self._metadata_manager.get_equipment_data()
        for callback in self.initialise_callbacks:
            callback(equipment_data)
        self._running = True

    def stop(self):
        self._running = False
    
    def is_running(self):
        return self._running
    
    @property
    def initialise_callbacks(self) -> list[Callable]:
        """
        Return the initialisation callbacks.
        
        Returns:
            A list of callable functions for initialisation.
        """
        return self._initialise_callbacks

    def add_initialise_callback(self, callback: Callable) -> None:
        """
        Add a callback function to the initialise callbacks.

        Args:
            callback: The callback function to be added.
        """
        self._initialise_callbacks.append(callback)

    def remove_initialise_callback(self, callback: Callable) -> None:
        """
        Remove a initialise callback function.

        Args:
            callback: The callback function to be removed.
        """
        self._initialise_callbacks.remove(callback)

    @property
    def measurement_callbacks(self) -> list[Callable]:
        """
        Return the measurement callbacks.
        
        Returns:
            A list of callable functions for measurements.
        """
        return self._measurement_callbacks

    def add_measurement_callback(self, callback: Callable) -> None:
        """
        Add a callback function to the measurement callbacks.

        Args:
            callback: The callback function to be added.
        """
        self._measurement_callbacks.append(callback)

    def remove_measurement_callback(self, callback: Callable) -> None:
        """
        Remove a measurement callback.

        Args:
            callback: The callback function to be removed.
        """
        self._measurement_callbacks.remove(callback)

    def set_error_holder(self,error_holder):
        self._error_holder = error_holder

    def _cast_callbacks(self, callbacks: Optional[Callable]) -> List[Callable]:
        """Ensure the callbacks are cast into a list."""
        if callbacks is None:
            return []
        elif not isinstance(callbacks, (list, set, tuple)):
            return [callbacks]
        return list(callbacks)

    def _initiate_callbacks(self, callbacks: List[Callable], 
                            data: dict = None) -> None:
        """Trigger all the registered callbacks."""
        for callback in callbacks:
            callback(data)

    def _handle_exception(self,exception):
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception