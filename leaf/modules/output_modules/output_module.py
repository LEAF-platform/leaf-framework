from abc import ABC
from abc import abstractmethod
import time
from typing import Optional
from typing import Any

from leaf.error_handler.exceptions import AdapterLogicError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import LEAFError
from leaf.error_handler.error_holder import ErrorHolder


class OutputModule(ABC):
    """
    Abstract base class for output modules responsible for transmitting
    data to external systems such as databases or network services.
    Supports fallback behavior and connection management.
    """

    def __init__(
        self,
        fallback: Optional["OutputModule"] = None,
        error_holder: Optional[ErrorHolder] = None
    ) -> None:
        """
        Initialize the OutputModule.

        Args:
            fallback (Optional[OutputModule]): Optional fallback output module.
            error_holder (Optional[ErrorHolder]): Error tracking mechanism.

        Raises:
            AdapterLogicError: If fallback is not a valid OutputModule.
        """
        if fallback is not None and not isinstance(fallback, OutputModule):
            raise AdapterLogicError("Output fallback must be an OutputModule.")

        self._fallback: Optional[OutputModule] = fallback
        self._error_holder: Optional[ErrorHolder] = error_holder
        self._enabled: Optional[float] = None

    @abstractmethod
    def transmit(self, topic: str, data: str) -> None:
        """
        Transmit data to the output system.

        Args:
            topic (str): The topic or destination identifier.
            data (str): Serialized data to be transmitted.
        """
        pass

    @abstractmethod
    def pop(self, key: Optional[str] = None) -> Any:
        """
        Retrieve and remove a stored message.

        Args:
            key (Optional[str]): Specific key to pop from buffer.

        Returns:
            Any: Retrieved message or None.
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
        Close the connection to the output system.
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check the output system connection status.

        Returns:
            bool: True if connected, False otherwise.
        """
        pass

    def flush(self, topic: str) -> None:
        """
        Flush any held data from the system, if implemented.

        Args:
            topic (str): Topic to flush.
        """
        if self._fallback is not None:
            self._fallback.flush(topic)

    def subscribe(self, topic: str) -> None:
        """
        Listen on the specified topic if supported by the module.

        Args:
            topic (str): Topic to subscribe to.
        """
        if self._fallback is not None:
            self._fallback.subscribe(topic)

    def fallback(self, topic: str, data: str) -> bool:
        """
        Attempt to transmit data using the fallback module.

        Args:
            topic (str): Topic for the message.
            data (str): Data to transmit.

        Returns:
            bool: True if fallback succeeded, False otherwise.
        """
        if self._fallback is not None:
            return self._fallback.transmit(topic, data)
        else:
            self._handle_exception(ClientUnreachableError(
                "Cannot store data, no output mechanisms available."
            ))
            return False

    def set_fallback(self, fallback: "OutputModule") -> None:
        """
        Set a new fallback module.

        Args:
            fallback (OutputModule): The fallback output module.

        Raises:
            AdapterLogicError: If fallback is not a valid OutputModule.
        """
        if not isinstance(fallback, OutputModule):
            self._handle_exception(AdapterLogicError(
                "Can't set fallback to a non-output module."
            ))
        else:
            self._fallback = fallback

    def is_enabled(self) -> bool:
        """
        Check whether output is enabled.

        Returns:
            bool: True if enabled, False otherwise.
        """
        return self._enabled is None

    def get_disabled_time(self) -> Optional[float]:
        """
        Get the timestamp when the output was disabled.

        Returns:
            Optional[float]: Timestamp, or None if not disabled.
        """
        return self._enabled

    def enable(self) -> None:
        """
        Re-enable output transmission.
        """
        self._enabled = None

    def disable(self) -> None:
        """
        Disable output transmission to prevent data dispatch.
        """
        self._enabled = time.time()

    def pop_all_messages(self) -> Any:
        """
        Yield all messages currently held by the module and its fallback.

        Yields:
            Any: Stored messages one at a time.
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
        Handle exceptions by forwarding to the error holder or raising.

        Args:
            exception (LEAFError): Error to be reported or raised.
        """
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception
