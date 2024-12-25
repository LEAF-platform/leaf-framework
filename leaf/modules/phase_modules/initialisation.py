from typing import Optional, Any
from leaf.modules.phase_modules.control import ControlPhase

class InitialisationPhase(ControlPhase):
    """
    A phase adapter responsible for handling data when the 
    adapter initialises. Inherits from ControlPhase.
    """
    
    def __init__(self,metadata_manager=None,
                 error_holder=None) -> None:
        """
        Initialise the InitialisationPhase with 
        the output adapter and metadata manager.

        Args:
            output_module: The OutputModule used to transmit data.
            metadata_manager: Manages metadata associated with the phase.
        """
        phase_term = metadata_manager.details
        super().__init__(phase_term, 
                         metadata_manager=metadata_manager,
                         error_holder=error_holder)

    def update(self, data: Optional[Any] = None):
        """
        Update the InitialisationPhase by 
        building the initialisation topic.

        Args:
            data: Data to be transmitted.
        """
        return super().update(data=data)
    
