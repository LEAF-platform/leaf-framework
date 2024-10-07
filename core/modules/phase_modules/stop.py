from core.modules.phase_modules.control import ControlPhase
from core.metadata_manager.metadata import metadata_manager
class StopPhase(ControlPhase):
    def __init__(self, output_adapter):
        term_builder = metadata_manager.experiment.stop
        super().__init__(output_adapter,term_builder)


    def update(self,data):
        running_action = metadata_manager.running()
        self._output.transmit(running_action,False,retain=True)
        start_action = metadata_manager.experiment.start()
        self._output.transmit(start_action,None,retain=True)
        super().update(data)