

class PhaseModule:
    def __init__(self,output_adapter,term_builder,
                 metadata_manager,interpreter=None):
        super().__init__()
        self._output = output_adapter
        self._interpreter = interpreter
        self._term_builder = term_builder
        self._metadata_manager = metadata_manager

    def set_interpreter(self, interpreter):
        self._interpreter = interpreter

    def update(self,data=None,retain=False,**kwargs):
        action = self._term_builder(**kwargs)
        self._output.transmit(action,data,retain=retain)