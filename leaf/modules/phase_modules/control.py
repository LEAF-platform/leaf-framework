from typing import Optional, Any
from leaf.modules.phase_modules.phase import PhaseModule
from leaf.modules.output_modules.output_module import OutputModule
from leaf.metadata_manager.metadata import MetadataManager
class ControlPhase(PhaseModule):
    """
    Handles control-related phases in a process. 
    Inherits from PhaseModule, allowing custom behavior for 
    controlling equipment during different phases such as 
    initialisation or stopping.
    """
    
    def __init__(self, 
                 output_module: OutputModule, 
                 phase_term: str, 
                 metadata_manager: MetadataManager,
                 error_holder=None) -> None:
        """
        Initialise the ControlPhase with the output adapter, 
        phase term, and metadata manager.

        Args:
            output_module (OutputModule): The OutputModule used to 
                          transmit data.
            phase_term (str): The term representing the control 
                       phase action.
            metadata_manager (MetadataManager): Manages metadata 
                             associated with the phase.
        """
        super().__init__(output_module, phase_term, metadata_manager,
                         error_holder=error_holder)

    def update(self, data: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Update the ControlPhase, transmitting data 
        via the output adapter.

        Args:
            data (Optional[Any]): Optional data to be transmitted.
            **kwargs (Any): Additional arguments to build the action term.
        """
        super().update(data, retain=True, **kwargs)
