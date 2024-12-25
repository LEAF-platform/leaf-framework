from leaf_register.metadata import MetadataManager
from abc import ABC
from abc import abstractmethod

class EventWatcher(ABC):
    """
    Aims to monitor and extract specific information from the equipment. 
    It is designed to detect and handle events, such as when equipment 
    provides measurements by writing to a file or any other 
    observable event. 
    """
    def __init__(self, term_map, metadata_manager: MetadataManager, 
                 callbacks = None, error_holder=None) -> None:
        """
        Initialise the EventWatcher instance.

        Args:
            metadata_manager: An instance of MetadataManager 
                              to manage equipment data.
        """
        self._metadata_manager = metadata_manager
        self._error_holder = error_holder
        self._running = False
        if callbacks is None:
            self._callbacks = []
        else:
            self._callbacks = callbacks
        
        if self.start not in term_map:
            term_map[self.start] = self._metadata_manager.details
        self._term_map = term_map

    @abstractmethod
    def start(self) -> None:
        """
        Start the EventWatcher and trigger the initialise callbacks.
        """
        equipment_data = self._metadata_manager.get_equipment_data()
        self._running = True
        return self._dispatch_callback(self.start,equipment_data)

    def add_callback(self,callback):
        self._callbacks.append(callback)
        
    def stop(self):
        self._running = False
    
    def is_running(self):
        return self._running
    
    def set_error_holder(self,error_holder):
        self._error_holder = error_holder

    def _handle_exception(self,exception):
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception

    def _dispatch_callback(self,function,data):
        if function not in self._term_map:
            # TODO
            excp = "STUB"
            self._handle_exception(excp)
            return 
        for cb in self._callbacks:
            cb(self._term_map[function],data)