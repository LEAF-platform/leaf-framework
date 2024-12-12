from typing import Any
from leaf.modules.phase_modules.control import ControlPhase
from leaf.modules.output_modules.output_module import OutputModule
from leaf_register.metadata import MetadataManager


class StartPhase(ControlPhase):
    """
    A ControlPhase responsible for starting the process by transmitting
    the necessary actions and setting the running status. Inherits from
    ControlPhase.
    """
    
    def __init__(self, output_adapter: OutputModule, 
                 metadata_manager: MetadataManager,
                 error_holder=None) -> None:
        """
        Initialize the StartPhase with the output adapter and metadata
        manager.

        Args:
            output_adapter (OutputModule): The OutputAdapter used to 
                          transmit data.
            metadata_manager (MetadataManager): Manages metadata 
                          associated with the phase.
        """
        term_builder = metadata_manager.experiment.start
        super().__init__(output_adapter, term_builder, metadata_manager,
                         error_holder=error_holder)

    def update(self, data: Any) -> None:
        """
        Update the StartPhase by transmitting actions to set the
        equipment as running.

        Args:
            data (Any): Data to be transmitted.
        """
        running_action = self._metadata_manager.running()
        self._output.transmit(running_action, True, retain=True)
        stop_action = self._metadata_manager.experiment.stop()
        self._output.transmit(stop_action, None, retain=True)
        
        if self._interpreter is not None:
            data = self._interpreter.metadata(data)
        
        super().update(data)
