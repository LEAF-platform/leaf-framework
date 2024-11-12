import os
from typing import Union, List, Optional
from core.adapters.equipment_adapter import EquipmentAdapter
from core.metadata_manager.metadata import MetadataManager
from core.modules.output_modules.output_module import OutputModule
from core.modules.process_modules.process_module import ProcessModule
from core.error_handler.error_holder import ErrorHolder
from core.adapters.equipment_adapter import AbstractInterpreter 

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'bioreactor.json')

class Bioreactor(EquipmentAdapter):
    """
    Subclass of EquipmentAdapter designed to manage bioreactor equipment.

    Initializes and configures the bioreactor, 
    adding bioreactor-specific metadata to the metadata manager.
    """

    def __init__(self, 
                 instance_data: dict, 
                 watcher: OutputModule, 
                 process_modules: Union[ProcessModule, List[ProcessModule]], 
                 interpreter: AbstractInterpreter, 
                 metadata_manager: Optional[MetadataManager] = None, 
                 error_holder: Optional[ErrorHolder] = None):
        """
        Initialize the Bioreactor instance.

        Args:
            instance_data (dict): Data specific to this bioreactor instance.
            watcher (OutputModule): Input module that monitors events or data.
            process_modules (Union[ProcessModule, List[ProcessModule]]): 
                A list or a single instance of ProcessModules for processing data.
            interpreter (AbstractInterpreter): An interpreter object to process the data.
            metadata_manager (Optional[MetadataManager]): Optional metadata manager instance 
                (defaults to new MetadataManager if None).
            error_holder (Optional[ErrorHolder]): Optional error holder instance.
        """
        # If no metadata manager is provided, instantiate a new one
        if metadata_manager is None:
            metadata_manager = MetadataManager()
        
        # Initialize the parent EquipmentAdapter
        super().__init__(instance_data, watcher, 
                         process_modules, interpreter,
                         metadata_manager=metadata_manager,
                         error_holder=error_holder)
        
        # Add specific metadata for the bioreactor equipment
        self._metadata_manager.add_equipment_data(metadata_fn)
