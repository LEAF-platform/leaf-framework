import os
from core.modules.phase_modules.measure import MeasurePhase
from core.metadata_manager.metadata import metadata_manager
class MeasurementPhase(MeasurePhase):
    def __init__(self, output_adapter,measurements):
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter,term_builder,measurements)


    def update(self,data):
        return super().update(data)