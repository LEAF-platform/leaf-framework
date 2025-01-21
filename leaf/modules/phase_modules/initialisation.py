from typing import Optional, Any
from leaf.modules.phase_modules.control import ControlPhase
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder

class InitialisationPhase(ControlPhase):
    """
    A phase adapter responsible for handling data when the adapter initializes.
    Inherits from ControlPhase.
    """

    def __init__(
        self,
        metadata_manager: Optional[MetadataManager] = None,
        error_holder: Optional[ErrorHolder] = None,
    ) -> None:
        """
        Initialize the InitialisationPhase with metadata manager and error holder.

        Args:
            metadata_manager (Optional[MetadataManager]): Manages metadata associated with the phase.
            error_holder (Optional[ErrorHolder]): Optional, an error holder to manage phase errors.
        """
        phase_term = metadata_manager.details
        super().__init__(
            phase_term, metadata_manager=metadata_manager, 
            error_holder=error_holder
        )

    def update(self, data: Optional[Any] = None) -> list:
        """
        Update the InitialisationPhase by building the initialization topic.

        Args:
            data (Optional[Any]): Data to be transmitted.

        Returns:
            list: A list of tuples containing the action terms and data.
        """
        return super().update(data=data)
