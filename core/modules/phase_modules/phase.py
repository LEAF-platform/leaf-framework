

class PhaseModule:
    def __init__(self,output_adapter,term_builder,
                 measurements=None,interpreter=None):
        super().__init__()
        self._output = output_adapter
        if measurements is None:
            self._measurements = []
        elif not isinstance(measurements,(list,set,tuple)): 
            self._measurements = [measurements]
        else:
            self._measurements = measurements
        self._interpreter = interpreter
        self._term_builder = term_builder

    def set_interpreter(self, interpreter):
        self._interpreter = interpreter

    def update(self,data=None,retain=False,**kwargs):
        action = self._term_builder(**kwargs)
        self._output.transmit(action,data,retain=retain)