from core.modules.phase_modules.control import ControlPhase

class StopPhase(ControlPhase):
    """
    A ControlPhase responsible for stopping the process by transmitting
    the necessary actions and setting the running status.
    Inherits from ControlPhase.
    """
    
    def __init__(self, output_adapter, metadata_manager) -> None:
        """
        Initialise the StopPhase with the output adapter 
        and metadata manager.

        Args:
            output_adapter: The OutputAdapter used to transmit data.
            metadata_manager: Manages metadata associated with the phase.
        """
        term_builder = metadata_manager.experiment.stop
        super().__init__(output_adapter, term_builder, metadata_manager)

    def update(self, data):
        """
        Update the StopPhase by transmitting actions to stop the process.

        Args:
            data: Data to be transmitted.
        """
        running_action = self._metadata_manager.running()
        self._output.transmit(running_action, False, retain=True)
        start_action = self._metadata_manager.experiment.start()
        self._output.transmit(start_action, None, retain=True)
        super().update(data)
