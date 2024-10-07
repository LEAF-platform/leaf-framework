from core.modules.process_modules.process_module import ProcessModule

class ContinousProcess(ProcessModule):
    def __init__(self,phase):
        if isinstance(phase,(list,tuple,set)):
            raise ValueError(f'Continous process may only have one phase.')
        super().__init__([phase])