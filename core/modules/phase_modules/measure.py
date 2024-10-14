from core.modules.phase_modules.phase import PhaseModule

class MeasurePhase(PhaseModule):
    def __init__(self,output_adapter,metadata_manager):
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter,term_builder,metadata_manager)

    def update(self,data=None,**kwargs):
        if self._interpreter is not None:
            exp_id = self._interpreter.id
            if exp_id is None:
                raise ValueError(f'Trying to transmit measurements outside of experiment.')
            result = self._interpreter.measurement(data)
            if result is None:
                super().update(data,**kwargs)
            if isinstance(data,list):
                md,data = result
                for measurement,data in data.items():
                    action = self._term_builder(experiment_id=exp_id,
                                                measurement=measurement)
                    if isinstance(data,list):
                        for d in data:
                            self._output.transmit(action,[md,d])
                    else:
                        self._output.transmit(action,[md,data])
            else:
                # Need to figure out what other adapter may produce...  
                action = self._term_builder(experiment_id=exp_id,
                                            measurement="unknown")
                self._output.transmit(action,data)
        else:
            super().update(data,**kwargs)    
        