import time

from core.modules.phase_modules.phase import PhaseModule
class MeasurePhase(PhaseModule):
    def __init__(self,output_adapter,metadata_manager,stagger_transmit=False):
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter,term_builder,metadata_manager)
        self._stagger_transmit = stagger_transmit


    def update(self,data=None,**kwargs):
        if self._interpreter is not None:
            exp_id = self._interpreter.id
            if exp_id is None:
                raise ValueError(f'Trying to transmit measurements outside of experiment.')
            result = self._interpreter.measurement(data)
            if result is None:
                super().update(data,**kwargs)
            if isinstance(result,dict):
                if self._stagger_transmit:
                    for measurement_type,measurements in result["fields"].items():
                        for measurement in measurements:
                            action = self._term_builder(experiment_id=exp_id,
                                                        measurement=measurement_type)
                            measurement["timestamp"] = result["timestamp"]
                            self._output.transmit(action,measurement)
                else:
                    action = self._term_builder(experiment_id=exp_id,
                                                measurement=result["measurement"])
                    
                    self._output.transmit(action,result)
            else:
                action = self._term_builder(experiment_id=exp_id,
                                            measurement="unknown")
                self._output.transmit(action,result)
        else:
            super().update(data,**kwargs)    
        