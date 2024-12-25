class ProcessModule:
    """
    A container for managing and running a specific process 
    within an EquipmentAdapter. A ProcessModule may consist 
    of multiple PhaseModules, each controlling a particular 
    phase of the process and enables grouping multiple phases
    under one process for better organisation and execution.
    """
    
    def __init__(self,output, phases, 
                 metadata_manager=None,
                 error_holder=None):
        """
        Initialise the ProcessModule with a collection of phases.

        Args:
            phases: A list of PhaseModules that represent 
            different phases of the process.
        """
        super().__init__()
        if not isinstance(phases, (list, set, tuple)):
            phases = [phases]
        self._output = output
        self._phases = phases
        self._error_holder=error_holder
        self._metadata_manager = metadata_manager

    def stop(self):
        for phase in self._phases:
            term = phase.get_term()
            if self._metadata_manager.is_complete_topic(term):
                self._output.flush(term)

    def process_input(self,topic,data):
        for phase in self._phases:
            if phase.is_activated(topic):
                for topic,data in phase.update(data):
                    self._output.transmit(topic,data)

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

    def set_metadata_manager(self,manager):
        self._metadata_manager = manager
        [p.set_metadata_manager(manager) for p in self._phases]

