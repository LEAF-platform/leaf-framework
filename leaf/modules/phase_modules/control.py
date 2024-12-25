from typing import Any
from leaf.modules.phase_modules.phase import PhaseModule

class ControlPhase(PhaseModule):
    """
    Handles control-related phases in a process. 
    Inherits from PhaseModule, allowing custom behavior for 
    controlling equipment during different phases such as 
    initialisation or stopping.
    """
    
    def __init__(self,phase_term: str, 
                 metadata_manager=None,
                 error_holder=None) -> None:
        """
        Initialise the ControlPhase with the
        phase term and metadata manager.

        Args:
            phase_term (str): The term representing the control 
                       phase action.
            metadata_manager (MetadataManager): Manages metadata 
                             associated with the phase.
        """
        super().__init__(phase_term, metadata_manager=metadata_manager,
                         error_holder=error_holder)

    def update(self, data=None,**kwargs: Any) -> None:
        """
        Builds the control phase term.
        Args:
            **kwargs (Any): Additional arguments to 
                            build the action term.
        """
        return super().update(data=data,**kwargs)
