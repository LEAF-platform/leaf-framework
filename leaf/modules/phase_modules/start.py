from typing import Any
from leaf.modules.phase_modules.control import ControlPhase
from leaf.modules.output_modules.output_module import OutputModule


class StartPhase(ControlPhase):
    """
    A ControlPhase responsible for starting the process by transmitting
    the necessary actions and setting the running status. Inherits from
    ControlPhase.
    """
    
    def __init__(self,metadata_manager: None,
                 error_holder=None) -> None:
        """
        Initialize the StartPhase with the output adapter and metadata
        manager.

        Args:
            output_adapter (OutputModule): The OutputAdapter used to 
                          transmit data.
            metadata_manager (MetadataManager): Manages metadata 
                          associated with the phase.
        """
        term_builder = metadata_manager.experiment.start
        super().__init__(term_builder, metadata_manager=metadata_manager,
                         error_holder=error_holder)

    def update(self, data: Any) -> None:
        """
        Update the StartPhase by transmitting actions to set the
        equipment as running.

        Args:
            data (Any): Data to be transmitted.
        """
        if self._interpreter is not None:
            data = self._interpreter.metadata(data)
        data = super().update(data)
        data += [(self._metadata_manager.running(),True)]
        data += [(self._metadata_manager.experiment.stop(),None)] 
        return data
        

