from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.measure import MeasurePhase


class MeasurementPhase(MeasurePhase):
    def __init__(self, output_adapter,measurements):
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter,term_builder,measurements)


    def update(self,data):
        return super().update(data)