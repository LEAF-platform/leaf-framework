from typing import Any
from leaf.modules.phase_modules.control import ControlPhase
from leaf.modules.output_modules.output_module import OutputModule
from leaf.metadata_manager.metadata import MetadataManager


class StopPhase(ControlPhase):
    """
    A ControlPhase responsible for stopping the process by transmitting
    the necessary actions and setting the running status.
    Inherits from ControlPhase.
    """
    
    def __init__(self, output_adapter: OutputModule, 
                 metadata_manager: MetadataManager,
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
        super().__init__(output_adapter, term_builder, metadata_manager,
                         error_holder=error_holder)

    def update(self, data: Any) -> None:
        """
        Update the StopPhase by transmitting actions to stop the process.

        Args:
            data (Any): Data to be transmitted.
        """
        running_action = self._metadata_manager.running()
        self._output.transmit(running_action, False, retain=True)
        start_action = self._metadata_manager.experiment.start()
        self._output.transmit(start_action, None, retain=True)
        super().update(data)
