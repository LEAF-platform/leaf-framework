from typing import Any
from core.modules.process_modules.process_module import ProcessModule
from core.error_handler.exceptions import AdapterBuildError
class DiscreteProcess(ProcessModule):
    """
    ProcessModule for processes with multiple phases.
    In a discrete process, there are distinct phases, 
    such as start, measurement, and stop phases. 
    Discrete processes dont actually set the phase in 
    anyway but are collections for I/O actions.
    """
    
    def __init__(self, phases: Any):
        """
        Initialise the DiscreteProcess with multiple phases.

        Args:
            phases: A collection of PhaseModules representing 
                    the different phases of the discrete process.

        Raises:
            ValueError: If only one phase is provided. 
                        Use ContinousProcess instead.
        """
        super().__init__(phases)
        if not isinstance(phases, (list, tuple, set)) or len(phases) <= 1:
            raise AdapterBuildError(f'''Discrete process should have 
                             more than one phase. Use continous 
                             process instead.''')
        
