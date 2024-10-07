from abc import abstractmethod


class OutputModule:
    def __init__(self,):
        super().__init__()

    @abstractmethod
    def transmit(self,topic):
        pass

    @abstractmethod
    def get_existing_ids(self):
        pass
