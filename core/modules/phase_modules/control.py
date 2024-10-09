from core.modules.phase_modules.phase import PhaseModule

class ControlPhase(PhaseModule):
    def __init__(self,output_adapter,phase_term,metadata_manager):
        super().__init__(output_adapter,phase_term,metadata_manager)

    def update(self,data=None,**kwargs):
        if self._interpreter is not None:
            data = self._interpreter.metadata(data)
        super().update(data,retain=True,**kwargs)