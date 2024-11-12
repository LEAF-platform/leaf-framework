from typing import Optional, Any
from core.modules.phase_modules.control import ControlPhase
from core.modules.output_modules.output_module import OutputModule
from core.metadata_manager.metadata import MetadataManager

class InitialisationPhase(ControlPhase):
    """
    A phase adapter responsible for handling data when the 
    adapter initialises. Inherits from ControlPhase.
    """
    
    def __init__(self, 
                 output_module: OutputModule, 
                 metadata_manager: MetadataManager) -> None:
        """
        Initialise the InitialisationPhase with 
        the output adapter and metadata manager.

        Args:
            output_module: The OutputModule used to transmit data.
            metadata_manager: Manages metadata associated with the phase.
        """
        phase_term = metadata_manager.details
        super().__init__(output_module, phase_term, 
                         metadata_manager)

    def update(self, data: Optional[Any] = None):
        """
        Update the InitialisationPhase by transmitting 
        the data via the output adapter.

        Args:
            data: Data to be transmitted.
        """
        return super().update(data)
