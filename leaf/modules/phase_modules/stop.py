from typing import Any, Optional
from leaf.modules.phase_modules.control import ControlPhase
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder


class StopPhase(ControlPhase):
    """
    A ControlPhase responsible for stopping the process by building
    the necessary actions and setting the running status.
    Inherits from ControlPhase.
    """

    def __init__(
        self,
        metadata_manager: MetadataManager,
        error_holder: Optional[ErrorHolder] = None,
    ) -> None:
        """
        Initialize the StopPhase with metadata manager and error holder.

        Args:
            metadata_manager (MetadataManager): Manages metadata associated with the phase.
            error_holder (Optional[ErrorHolder]): Optional, an error holder to manage phase errors.
        """
        term_builder = metadata_manager.experiment.stop
        super().__init__(
            term_builder, metadata_manager=metadata_manager, 
            error_holder=error_holder
        )

    def update(self, data: Any) -> list:
        """
        Update the StopPhase by transmitting actions to set the equipment as stopped.

        Args:
            data (Any): Data to be transmitted.

        Returns:
            list: A list of tuples containing the action terms and data.
        """
        # Leaving unused until stop experiment metadata is agreed upon.
        # if self._interpreter is not None:
        #    data = self._interpreter.metadata(data)
        data = super().update(data)
        data += [(self._metadata_manager.running(), False)]
        data += [(self._metadata_manager.experiment.start(), None)]
        return data
