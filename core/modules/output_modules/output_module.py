from abc import abstractmethod


class OutputModule:
    def __init__(self,fallback=None):
        if fallback is not None and not isinstance(fallback,OutputModule):
            raise ValueError("Fallback argument must be a OutputModule.")
        self._fallback = fallback

    @abstractmethod
    def transmit(self,topic,data):
        pass

    @abstractmethod
    def get_existing_ids(self):
        pass
    
    def fallback(self,topic,data=None):
        self._fallback.transmit(topic,data)
