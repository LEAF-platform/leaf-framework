class PhaseModule:
    """
    Manages distinct phases within a ProcessAdapter and ensures that 
    tasks are executed in the correct sequence for each phase.PhaseModule 
    allows related processes to be grouped together. For example, equipment 
    may run multiple processes, each consisting of discrete phases 
    (start, measurement, stop).
    """
    
    def __init__(self, output_adapter, term_builder, metadata_manager, interpreter=None):
        """
        Initialise the PhaseModule with the necessary components.

        Args:
            output_adapter: The OutputAdapter used to transmit data.
            term_builder: A function from the metadata_manager to construct the 
                          action term for each phase.
            metadata_manager: Manages metadata associated with the phase.
            interpreter: Optional, an interpreter to process data if needed.
        """
        super().__init__()
        self._output = output_adapter
        self._interpreter = interpreter
        self._term_builder = term_builder
        self._metadata_manager = metadata_manager

    def set_interpreter(self, interpreter):
        """
        Set or update the interpreter for the phase.

        Args:
            interpreter: The interpreter to be used for processing data.
        """
        self._interpreter = interpreter

    def update(self, data=None, retain=False, **kwargs):
        """
        Trigger the update action for the phase, 
        transmitting data using the OutputAdapter. Within the larger 
        system, this function would likely be passed to an 
        InputModule as a callback.

        Args:
            data: Optional data to be transmitted.
            retain: Whether to retain the transmitted data.
            **kwargs: Additional arguments used to build the action term.
        """
        action = self._term_builder(**kwargs)
        self._output.transmit(action, data, retain=retain)
