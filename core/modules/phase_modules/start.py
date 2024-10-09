from core.modules.phase_modules.control import ControlPhase
class StartPhase(ControlPhase):
    def __init__(self, output_adapter,metadata_manager):
        term_builder = metadata_manager.experiment.start
        super().__init__(output_adapter,term_builder,metadata_manager)

    def update(self,data):
        running_action = self._metadata_manager.running()
        self._output.transmit(running_action,True,retain=True)
        stop_action = self._metadata_manager.experiment.stop()
        self._output.transmit(stop_action,None,retain=True)
        super().update(data)