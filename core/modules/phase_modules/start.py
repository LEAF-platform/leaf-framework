from core.modules.phase_modules.control import ControlPhase

class StartPhase(ControlPhase):
    """
    A ControlPhase responsible for starting the process by transmitting
    the necessary actions and setting the running status. Inherits from 
    ControlPhase.
    """
    
    def __init__(self, output_adapter, metadata_manager) -> None:
        """
        Initialise the StartPhase with the output adapter and 
        metadata manager.

        Args:
            output_adapter: The OutputAdapter used to transmit data.
            metadata_manager: Manages metadata associated with the phase.
        """
        term_builder = metadata_manager.experiment.start
        super().__init__(output_adapter, term_builder, metadata_manager)

    def update(self, data):
        """
        Update the StartPhase by transmitting actions 
        to set the equipment as running.

        Args:
            data: Data to be transmitted.
        """
        running_action = self._metadata_manager.running()
        self._output.transmit(running_action, True, retain=True)
        stop_action = self._metadata_manager.experiment.stop()
        self._output.transmit(stop_action, None, retain=True)
        super().update(data)
