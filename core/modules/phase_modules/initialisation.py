from core.modules.phase_modules.control import ControlPhase

class InitialisationPhase(ControlPhase):
    """
    A phase adapter responsible for handling data when the 
    adapter initialises. Inherits from ControlPhase.
    """
    
    def __init__(self, output_adapter, metadata_manager) -> None:
        """
        Initialise the InitialisationPhase with 
        the output adapter and metadata manager.

        Args:
            output_adapter: The OutputAdapter used to transmit data.
            metadata_manager: Manages metadata associated with the phase.
        """
        phase_term = metadata_manager.details
        super().__init__(output_adapter, phase_term, metadata_manager)

    def update(self, data):
        """
        Update the InitialisationPhase by transmitting 
        the data via the output adapter.

        Args:
            data: Data to be transmitted.
        """
        return super().update(data)
