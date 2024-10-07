import os
from core.modules.phase_modules.control import ControlPhase
from core.metadata_manager.metadata import metadata_manager
class InitialisationPhase(ControlPhase):
    def __init__(self, output_adapter):
        phase_term = metadata_manager.details
        super().__init__(output_adapter,phase_term)


    def update(self,data):
        return super().update(data)