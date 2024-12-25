from leaf.modules.process_modules.process_module import ProcessModule
from leaf.error_handler.exceptions import AdapterBuildError
class ContinousProcess(ProcessModule):
    """
    A ProcessModule for processes with a single phase.
    In a continuous process, there is only one phase that 
    runs continuously,such as a measurement or control 
    phase that remains active throughout the process.
    """
    
    def __init__(self,output, phase,metadata_manager=None,
                 error_holder=None):
        """
        Initialise the ContinousProcess with a single phase.

        Args:
            phase: A single PhaseModule representing the continuous phase.

        Raises:
            ValueError: If more than one phase is provided.
        """
        if isinstance(phase, (list, tuple, set)):
            raise AdapterBuildError(f'Continous process may only have one phase.')
        super().__init__(output,[phase],metadata_manager=metadata_manager,
                         error_holder=error_holder)

        
