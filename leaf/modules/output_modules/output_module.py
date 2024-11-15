from abc import abstractmethod
from abc import ABC
import logging

from leaf.error_handler.exceptions import AdapterLogicError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import LEAFError
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.WARNING)

from leaf.modules.input_modules.file_watcher import logger


class OutputModule(ABC):
    """
    Abstract class that defines the structure for output adapters and are
    responsible for outputting information using a particular system. 
    For example, save them to a local database or publish them to an 
    external service. A fallback mechanism is also supported, where if 
    the primary OutputModule fails to transmit the data, a secondary 
    fallback module can be used to handle the output.
    """
    
    def __init__(self, fallback: str|None=None,
                 error_holder:ErrorHolder=None) -> None:
        """
        Initialise the OutputModule with an 
        optional fallback OutputModule.

        Args:
            fallback: Another OutputModule that will be 
                      used as a fallback in case the current 
                      module fails to transmit the data.

        Raises:
            ValueError: If the fallback argument is not an instance of OutputModule.
        """
        if fallback is not None and not isinstance(fallback, OutputModule):
            raise AdapterLogicError("Output fallback argument must be an OutputModule.")
        self._fallback = fallback
        self._error_holder = error_holder
        self._enabled = True

    @abstractmethod
    def transmit(self, topic: str, data: str) -> None:
        """
        Abstract method to transmit data to an external system.

        Args:
            topic: The topic or destination where the 
                   data should be transmitted.
            data: The data to be transmitted.
        """
        logger.error("Method 'transmit' must be implemented in a subclass.")
        pass
    
    def fallback(self, topic:str , data:str) -> None:
        """
        Transmit the data using the fallback 
        OutputModule if the primary module fails.

        Args:
            topic: The topic or destination where 
                    the data should be transmitted.
            data: The data to be transmitted 
                    (optional, may be None).
        """
        if self._fallback is not None:
            return self._fallback.transmit(topic, data)
        else:
            self._handle_exception(ClientUnreachableError(f'Cant store data, no output mechanisms available'))
            return False

    def enable(self):
        '''
        Reenables an output transmitting.
        Only needs to be called if the disable 
        function has been called previously.
        '''
        logger.info(f'{self.__class__.__name__} is enabled as an output.')
        self._enabled = True

    def disable(self):
        '''
        Stops an output from transmitting.
        This will be used to disable output modules which arent 
        working for whatever reason to stop them locking the system.
        '''
        logger.warning(f'{self.__class__.__name__} is disabled as an output.')
        self._enabled = False

    def _handle_exception(self,exception:LEAFError):
        '''
        When exception is created add to error holder if 
        its set else raise it.
        '''
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception
        

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass