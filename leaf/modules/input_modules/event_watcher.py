from leaf_register.metadata import MetadataManager
from abc import ABC
from abc import abstractmethod
from typing import Optional, List, Callable, Any
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterLogicError

class EventWatcher(ABC):
    """
    Aims to monitor and extract specific information from
    the equipment. It is designed to detect and handle
    events, such as when equipment provides measurements
    by writing to a file or any other observable event.
    """
    def __init__(self, term_map: dict, 
                 metadata_manager: MetadataManager = None,
                 callbacks: Optional[List[Callable]] = None, 
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialise the EventWatcher instance.

        Args:
            term_map (dict): A mapping of the functions pertaining 
                             to events and terms
            metadata_manager (MetadataManager): An instance of 
                                                MetadataManager to 
                                                manage equipment data.
            callbacks (Optional[List[Callable]]): List of callbacks 
                                                  to execute on event 
                                                  triggers.
            error_holder (Optional[ErrorHolder]): Optional object to hold 
                                                  and manage errors.
        """
        self._metadata_manager = metadata_manager
        self._error_holder = error_holder
        self._running = False
        self._callbacks = callbacks if callbacks is not None else []

        if (self.start not in term_map and 
            self._metadata_manager is not None):
            term_map[self.start] = self._metadata_manager.details
        self._term_map = term_map

    @abstractmethod
    def start(self) -> None:
        """
        Start the EventWatcher and trigger the initialise callbacks.
        """
        equipment_data = self._metadata_manager.get_data()
        self._running = True
        return self._dispatch_callback(self.start, equipment_data)

    def add_callback(self, callback: Callable) -> None:
        """
        Add a new callback to the EventWatcher.

        Args:
            callback (Callable): The callback function to add.
        """
        self._callbacks.append(callback)

    def stop(self) -> None:
        """
        Stop the EventWatcher, halting further event detection.
        """
        self._running = False

    def is_running(self) -> bool:
        """
        Check if the EventWatcher is currently running.

        Returns:
            bool: True if running, False otherwise.
        """
        return self._running

    def set_error_holder(self, error_holder: ErrorHolder) -> None:
        """
        Set the error holder for managing exceptions.

        Args:
            error_holder (ErrorHolder): Object to hold and manage errors.
        """
        self._error_holder = error_holder

    def set_metadata_manager(self,metadata_manager):
        self._metadata_manager = metadata_manager

    def get_terms(self) -> list:
        """
        Retrieve terms mapped to the watcher functions.

        Returns:
            list: List of terms corresponding to mapped functions.
        """
        return [f() if callable(f) else f for 
                f in self._term_map.values()]

    def _handle_exception(self, exception: Exception) -> None:
        """
        Handle exceptions encountered during event processing.

        Args:
            exception (Exception): Exception to handle.
        """
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception

    def _dispatch_callback(self, function: Callable, 
                           data: Any) -> None:
        """
        Dispatch the associated callbacks for a given function.

        Args:
            function (Callable): The function triggering the callbacks.
            data (Any): Data to pass to the callbacks.
        """
        if function not in self._term_map:
            excp = AdapterLogicError("Function not mapped to terms.")
            self._handle_exception(excp)
            return
        for cb in self._callbacks:
            cb(self._term_map[function], data)