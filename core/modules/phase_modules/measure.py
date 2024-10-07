from core.modules.phase_modules.phase import PhaseModule

class MeasurePhase(PhaseModule):
    def __init__(self,output_adapter,term_builder,
                 measurements):
        super().__init__(output_adapter,term_builder,
                         measurements=measurements)

    def update(self,data=None,**kwargs):
        if self._interpreter is not None:
            # Standard terms perhaps??
            # Need to figure out here if how measurements are properly accessesd..
            data = self._interpreter.measurement(data,self._measurements)
            exp_id = self._interpreter.id
            print(exp_id)
            if exp_id is not None and "experiment_id" not in kwargs:
                kwargs["experiment_id"] = exp_id
        super().update(data,**kwargs)
        