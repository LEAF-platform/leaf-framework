from core.modules.measurement_modules.measurement_module import MeasurementModule

class Biomass(MeasurementModule):
    def __init__(self):
        super().__init__()

    def transform(self, measurement):
        return measurement