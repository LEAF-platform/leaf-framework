from core.modules.phase_modules.phase import PhaseModule

class ControlPhase(PhaseModule):
    """
    Handles control-related phases in a process. 
    Inherits from PhaseModule, allowing custom behavior for 
    controlling equipment during different phases such as 
    initialisation or stopping.
    """
    
    def __init__(self, output_adapter, phase_term, metadata_manager):
        """
        Initialise the ControlPhase with the output adapter, 
        phase term, and metadata manager.

        Args:
            output_adapter: The OutputAdapter used to transmit data.
            phase_term: The term representing the control phase action.
            metadata_manager: Manages metadata associated with the phase.
        """
        super().__init__(output_adapter, phase_term, metadata_manager)

    def update(self, data=None, **kwargs):
        """
        Update the ControlPhase, transmitting data 
        via the output adapter. If an interpreter is set, 
        it processes the data before transmission.

        Args:
            data: Optional data to be transmitted.
            **kwargs: Additional arguments to build the action term.
        """
        if self._interpreter is not None:
            data = self._interpreter.metadata(data)
        super().update(data, retain=True, **kwargs)
