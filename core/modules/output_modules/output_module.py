from abc import abstractmethod
from abc import ABC

class OutputModule(ABC):
    """
    Abstract class that defines the structure for output adapters and are 
    responsible for outputting information using a particular system. 
    For example, save them to a local database or publish them to an 
    external service. A fallback mechanism is also supported, where if 
    the primary OutputModule fails to transmit the data, a secondary 
    fallback module can be used to handle the output.
    """
    
    def __init__(self, fallback: str|None=None) -> None:
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
            raise ValueError("Fallback argument must be an OutputModule.")
        self._fallback = fallback

    @abstractmethod
    def transmit(self, topic: str, data: str) -> None:
        """
        Abstract method to transmit data to an external system.

        Args:
            topic: The topic or destination where the 
                   data should be transmitted.
            data: The data to be transmitted.
        """
        pass
    
    def fallback(self, topic:str , data:str|None =None) -> None:
        """
        Transmit the data using the fallback 
        OutputModule if the primary module fails.

        Args:
            topic: The topic or destination where 
                    the data should be transmitted.
            data: The data to be transmitted 
                    (optional, may be None).
        """
        self._fallback.transmit(topic, data)
