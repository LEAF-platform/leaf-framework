from abc import abstractmethod
from abc import ABC
import time
from typing import Optional,Any

from leaf.error_handler.exceptions import AdapterLogicError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import LEAFError
from leaf.error_handler.error_holder import ErrorHolder

class OutputModule(ABC):
    """
    Abstract class that defines the structure for output adapters and is
    responsible for outputting information using a particular system.
    For example, saving data to a local database or publishing to an
    external service. Supports a fallback mechanism if the primary
    OutputModule fails.
    """
    def __init__(self, fallback: Optional['OutputModule'] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize the OutputModule with an 
        optional fallback OutputModule.

        Args:
            fallback (Optional[OutputModule]): Used as a fallback if the 
                                                current module fails to 
                                                transmit data.
            error_holder (Optional[ErrorHolder]): Tracks and manages errors.

        Raises:
            AdapterLogicError: If the fallback is not an 
                                OutputModule instance.
        """ 
        if fallback is not None and not isinstance(fallback, OutputModule):
            raise AdapterLogicError("Output fallback must be an OutputModule.")
        self._fallback = fallback
        self._error_holder = error_holder
        self._enabled = None

    @abstractmethod
    def transmit(self, topic: str, data: str) -> None:
        """
        Abstract method to transmit data to an external system.

        Args:
            topic (str): The topic or destination for the data.
            data (str): The data to transmit.
        """
        pass

    @abstractmethod
    def pop(self, key: Optional[str] = None) -> Any:
        """
        Abstract method to retrieve and remove a record from the 
        output module.

        Args:
            key (Optional[str]): The key of the record to retrieve 
                                 and remove. If None, the method 
                                 should retrieve and remove a default 
                                 or random record.

        Returns:
            Any: The retrieved record, or None if no record exists.
        """
        pass

    @abstractmethod
    def connect(self) -> None:
        """
        Establish a connection to the output system.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnect from the output system.
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the module is connected to the output system.

        Returns:
            bool: True if connected, False otherwise.
        """
        pass

    def flush(self,topic:str) -> bool:
        """
        Flush any held transient data within the output module.
        """
        if self._fallback is not None:
            self._fallback.flush(topic)

    def subscribe(self,topic:str) -> bool:
        """
        Listens on any routes where available.
        """
        if self._fallback is not None:
            self._fallback.subscribe(topic)

    def fallback(self, topic: str, data: str) -> bool:
        """
        Transmit the data using the fallback OutputModule 
        if the primary fails.

        Args:
            topic (str): The topic or destination for the data.
            data (str): The data to transmit.

        Returns:
            bool: True if fallback transmission is successful, 
                  False otherwise.
        """
        if self._fallback is not None:
            return self._fallback.transmit(topic, data)
        else:
            self._handle_exception(ClientUnreachableError(
                "Cannot store data, no output mechanisms available."))
            return False

    def set_fallback(self,fallback):
        if not isinstance(fallback,OutputModule):
            self._handle_exception(AdapterLogicError(
                "Cant set fallback to non output module"))
        else:
            self._fallback = fallback

    def is_enabled(self) -> bool:
        return self._enabled is None

    def get_disabled_time(self) -> Optional[float]:
        return self._enabled

    def enable(self) -> None:
        """
        Re-enable output transmission if it was previously disabled.
        """
        self._enabled = None

    def disable(self) -> None:
        """
        Disable output transmission to prevent potential system locking.
        """
        self._enabled = time.time()

    def pop_all_messages(self) -> Any:
        """
        Yield all messages from the module and its fallback.

        Yields:
            Any: Messages from the module or fallback.
        """
        while True:
            message = self.pop()
            if message is None:
                break
            yield message
        if self._fallback is not None:
            yield from self._fallback.pop_all_messages()

    def _handle_exception(self, exception: LEAFError) -> None:
        """
        Handle exceptions by adding them to the error holder or 
        raising them.

        Args:
            exception (LEAFError): The exception to handle.
        """
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception
