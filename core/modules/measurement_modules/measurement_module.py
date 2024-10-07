from abc import abstractmethod

class MeasurementModule:
    def __init__(self,):
        super().__init__()

    @abstractmethod
    def transform(self,measurement):
        pass