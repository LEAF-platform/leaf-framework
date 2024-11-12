class ProcessModule:
    """
    A container for managing and running a specific process 
    within an EquipmentAdapter. A ProcessModule may consist 
    of multiple PhaseModules, each controlling a particular 
    phase of the process and enables grouping multiple phases
    under one process for better organisation and execution.
    """
    
    def __init__(self, phases,error_holder=None):
        """
        Initialise the ProcessModule with a collection of phases.

        Args:
            phases: A list of PhaseModules that represent 
            different phases of the process.
        """
        super().__init__()
        if not isinstance(phases, (list, set, tuple)):
            phases = [phases]
        self._phases = phases
        self._error_holder=error_holder
            
    def set_interpreter(self, interpreter):
        """
        Set or update the interpreter for 
        all phases within the process.

        Args:
            interpreter: The interpreter to 
            be set for each PhaseModule.
        """
        for p in self._phases:
            p.set_interpreter(interpreter)

    def set_error_holder(self, error_holder):
        self._error_holder = error_holder
        for p in self._phases:
            p.set_error_holder(error_holder)


