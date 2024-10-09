from core.modules.phase_modules.control import ControlPhase
class StopPhase(ControlPhase):
    def __init__(self, output_adapter,metadata_manager):
        term_builder = metadata_manager.experiment.stop
        super().__init__(output_adapter,term_builder,metadata_manager)


    def update(self,data):
        running_action = self._metadata_manager.running()
        self._output.transmit(running_action,False,retain=True)
        start_action = self._metadata_manager.experiment.start()
        self._output.transmit(start_action,None,retain=True)
        super().update(data)