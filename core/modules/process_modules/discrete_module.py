from core.modules.process_modules.process_module import ProcessModule

class DiscreteProcess(ProcessModule):
    def __init__(self,phases):
        if not isinstance(phases,(list,tuple,set)):
            raise ValueError(f'''Discrete process should have 
                             more than one phase. Use continous 
                             process instead.''')
        super().__init__(phases)