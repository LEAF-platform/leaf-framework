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
                 initialise_callbacks: Optional[List[Callable]] = None,
                 measurement_callbacks: Optional[List[Callable]] = None,
                 start_callbacks: Optional[List[Callable]] = None,
                 stop_callbacks: Optional[List[Callable]] = None) -> None:
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
        self._initialise_callbacks = self._cast_callbacks(initialise_callbacks)
        self._measurement_callbacks = self._cast_callbacks(measurement_callbacks)
        self._start_callbacks = self._cast_callbacks(start_callbacks)
        self._stop_callbacks = self._cast_callbacks(stop_callbacks)
    
    @abstractmethod
    def start(self) -> None:
        """
        Start the EventWatcher, triggering initialisation callbacks.
        Subclasses implement this to start monitoring events.
        """
        equipment_data = self._metadata_manager.get_equipment_data()
        for callback in self.initialise_callbacks:
            callback(equipment_data)

    def _cast_callbacks(self, callbacks: Optional[List[Callable]]) -> List[Callable]:
        """
        Ensure callbacks are in a list. If none, return an empty list.

        Args:
            callbacks: A callable or list/tuple/set of callables 
                       for a specific event.
        
        Returns:
            A list of callback functions.
        """
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

    @property
    def initialise_callbacks(self) -> List[Callable]:
        """
        Return the list of callbacks for initialisation.
        
        Returns:
            List of initialisation callables.
        """
        return self._initialise_callbacks

    def add_initialise_callback(self, callback: Callable) -> None:
        """
        Add a callback function to initialisation callbacks.

        Args:
            callback: Function to add to initialisation callbacks.
        """
        self._initialise_callbacks.append(callback)

    def remove_initialise_callback(self, callback: Callable) -> None:
        """
        Remove a callback function from initialisation callbacks.

        Args:
            callback: Function to remove from initialisation callbacks.
        """
        self._initialise_callbacks.remove(callback)

    @property
    def measurement_callbacks(self) -> List[Callable]:
        """
        Return list of callbacks for measurement events.
        
        Returns:
            List of measurement callables.
        """
        return self._measurement_callbacks

    def add_measurement_callback(self, callback: Callable) -> None:
        """
        Add a callback to measurement event callbacks.

        Args:
            callback: Function to add to measurement callbacks.
        """
        self._measurement_callbacks.append(callback)

    def remove_measurement_callback(self, callback: Callable) -> None:
        """
        Remove a callback from measurement event callbacks.

        Args:
            callback: Function to remove from measurement callbacks.
        """
        self._measurement_callbacks.remove(callback)

    @property
    def start_callbacks(self) -> List[Callable]:
        """
        Return list of callbacks for start events.
        
        Returns:
            List of start event callables.
        """
        return self._start_callbacks

    def add_start_callback(self, callback: Callable) -> None:
        """
        Add a callback to start event callbacks.

        Args:
            callback: Function to add to start callbacks.
        """
        self._start_callbacks.append(callback)

    def remove_start_callback(self, callback: Callable) -> None:
        """
        Remove a callback from start event callbacks.

        Args:
            callback: Function to remove from start callbacks.
        """
        self._start_callbacks.remove(callback)

    @property
    def stop_callbacks(self) -> List[Callable]:
        """
        Return list of callbacks for stop events.
        
        Returns:
            List of stop event callables.
        """
        return self._stop_callbacks

    def add_stop_callback(self, callback: Callable) -> None:
        """
        Add a callback to stop event callbacks.

        Args:
            callback: Function to add to stop callbacks.
        """
        self._stop_callbacks.append(callback)

    def remove_stop_callback(self, callback: Callable) -> None:
        """
        Remove a callback from stop event callbacks.

        Args:
            callback: Function to remove from stop callbacks.
        """
        self._stop_callbacks.remove(callback)
