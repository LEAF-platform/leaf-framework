from core.modules.phase_modules.phase import PhaseModule

class MeasurePhase(PhaseModule):
    def __init__(self,output_adapter,metadata_manager):
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter,term_builder,metadata_manager)

    def update(self,data=None,**kwargs):
        if self._interpreter is not None:
            md,data = self._interpreter.measurement(data,self._measurements)
            exp_id = self._interpreter.id
            if exp_id is None:
                raise ValueError(f'Trying to transmit measurements outside of experiment.')
            for k,v in data.items():
                action = self._term_builder(experiment_id=exp_id,
                                            measurement=k)
                self._output.transmit(action,[md,v])
        else:
            super().update(data,**kwargs)    
        