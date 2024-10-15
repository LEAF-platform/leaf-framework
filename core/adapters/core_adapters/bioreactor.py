import os

from core.adapters.equipment_adapter import EquipmentAdapter
from core.metadata_manager.metadata import MetadataManager

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'bioreactor.json')

class Bioreactor(EquipmentAdapter):
    """
    Subclass of EquipmentAdapter designed to manage bioreactor equipment.

    Initializes and configures the bioreactor, 
    adding bioreactor specific metadata to the metadata manager.
    """
    def __init__(self, instance_data, watcher, process_adapters,
                 interpreter, metadata_manager=None):
        """
        Initialise the Bioreactor instance.

        Args:
            instance_data: Data specific to this bioreactor instance.
            watcher: InputModule that watches or monitors events or data.
            process_adapters: A list or a single instance of ProcessaAdapters.
            interpreter: An interpreter object to process the data.
            metadata_manager: Optional metadata manager instance 
            (defaults to new MetadataManager if None).
        """
        if metadata_manager is None:
            metadata_manager = MetadataManager()
        super().__init__(instance_data, watcher, 
                         process_adapters, interpreter,
                         metadata_manager=metadata_manager)
        self._metadata_manager.add_equipment_data(metadata_fn)
