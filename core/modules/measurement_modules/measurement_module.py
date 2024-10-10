from abc import abstractmethod

class MeasurementModule:
    def __init__(self,term):
        self._term = term

    @abstractmethod
    def transform(self,measurement):
        pass