import os

from core.adapters.equipment_adapter import EquipmentAdapter
from core.metadata_manager.metadata import metadata_manager

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'bioreactor.json')

class Bioreactor(EquipmentAdapter):
    def __init__(self,instance_data,watcher,process_adapters,interpreter):
        super().__init__(instance_data,watcher,process_adapters,interpreter)
        metadata_manager.add_equipment_data(metadata_fn)