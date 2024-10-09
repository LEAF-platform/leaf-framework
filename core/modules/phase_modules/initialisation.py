import os
from core.modules.phase_modules.control import ControlPhase
class InitialisationPhase(ControlPhase):
    def __init__(self, output_adapter,metadata_manager):
        phase_term = metadata_manager.details
        super().__init__(output_adapter,phase_term,metadata_manager)


    def update(self,data):
        return super().update(data)