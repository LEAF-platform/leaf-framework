from core.modules.measurement_modules.measurement_module import MeasurementModule

class pH(MeasurementModule):
    def __init__(self,term):
        super().__init__(term)

    def transform(self, measurement):
        return measurement