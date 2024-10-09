from abc import abstractmethod
from abc import ABC

class OutputModule(ABC):
    def __init__(self,fallback=None):
        if fallback is not None and not isinstance(fallback,OutputModule):
            raise ValueError("Fallback argument must be a OutputModule.")
        self._fallback = fallback

    @abstractmethod
    def transmit(self,topic,data):
        pass
    
    def fallback(self,topic,data=None):
        self._fallback.transmit(topic,data)
