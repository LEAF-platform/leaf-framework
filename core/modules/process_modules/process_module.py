

class ProcessModule:
    def __init__(self,phases):
        super().__init__()
        if not isinstance(phases,(list,set,tuple)):
            phases = [phases]
        self._phases = phases
            
    def set_interpreter(self, interpreter):
        for p in self._phases:
            p.set_interpreter(interpreter)