from typing import Any
from leaf.modules.phase_modules.control import ControlPhase


class StopPhase(ControlPhase):
    """
    A ControlPhase responsible for stopping the process by building
    the necessary actions and setting the running status.
    Inherits from ControlPhase.
    """
    
    def __init__(self,metadata_manager: None,
                 error_holder=None) -> None:
        """
        Initialize the StopPhase with the output adapter and metadata
        manager.

        Args:
            output_adapter (OutputModule): The OutputAdapter used to
                          transmit data.
            metadata_manager (MetadataManager): Manages metadata 
                          associated with the phase.
        """
        term_builder = metadata_manager.experiment.stop
        super().__init__(term_builder, metadata_manager=metadata_manager,
                         error_holder=error_holder)

    def update(self, data: Any) -> None:
        """
        Update the StopPhase by transmitting actions to set the
        equipment as running.

        Args:
            data (Any): Data to be transmitted.
        """
        # Leaving unused until stop experiment metadata is agreed upon.
        #if self._interpreter is not None:
        #    data = self._interpreter.metadata(data)
        data = super().update(data)
        data += [(self._metadata_manager.running(),False)]
        data += [(self._metadata_manager.experiment.start(),None)] 
        return data