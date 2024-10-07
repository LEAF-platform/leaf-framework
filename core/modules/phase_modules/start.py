from core.modules.phase_modules.control import ControlPhase
from core.metadata_manager.metadata import metadata_manager

class StartPhase(ControlPhase):
    def __init__(self, output_adapter):
        term_builder = metadata_manager.experiment.start
        super().__init__(output_adapter,term_builder)


    def update(self,data):
        running_action = metadata_manager.running()
        self._output.transmit(running_action,True,retain=True)
        stop_action = metadata_manager.experiment.stop()
        self._output.transmit(stop_action,None,retain=True)
        super().update(data)