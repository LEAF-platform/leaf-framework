import logging
from typing import Optional, Callable, List
from core.metadata_manager.metadata import MetadataManager
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class EventWatcher(ABC):
    """
    Base class for monitoring and handling equipment events. Allows 
    registration of callbacks for initialisation, measurement, start, 
    and stop events, triggering each on event detection.
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
        Initialise the EventWatcher instance with callback lists 
        for various events and a metadata manager.
        
        Args:
            metadata_manager: Manages equipment data for event handling.
            initialise_callbacks: List of callbacks for initialisation.
            measurement_callbacks: List of callbacks for measurements.
            start_callbacks: List of callbacks for start events.
            stop_callbacks: List of callbacks for stop events.
        """
        self._metadata_manager = metadata_manager
        self._error_holder = error_holder
        self._running = False

    @abstractmethod
    def start(self) -> None:
        """
        Start the EventWatcher, triggering initialisation callbacks.
        Subclasses implement this to start monitoring events.
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
        Ensure callbacks are in a list. If none, return an empty list.

        Args:
            callbacks: A callable or list/tuple/set of callables 
                       for a specific event.
        
        Returns:
            A list of callback functions.
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
                            data: Optional[dict] = None) -> None:
        """
        Trigger each callback in the list, passing in optional event data.

        Args:
            callbacks: List of callback functions to execute.
            data: Optional event data dictionary for context.
        """
        for callback in callbacks:
            callback(data)

    def _handle_exception(self,exception):
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception