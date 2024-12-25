from typing import Optional, Callable, Any
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.adapters.equipment_adapter import AbstractInterpreter


class PhaseModule:
    """
    Manages distinct phases within a ProcessAdapter to ensure tasks
    execute in sequence for each phase. PhaseModule allows related
    processes to be grouped. For example, equipment may run multiple
    processes, each with discrete phases (start, measurement, stop).
    """
    
    def __init__(self,term_builder: Callable[..., str],
                 metadata_manager: Optional[MetadataManager] = None,
                 interpreter: Optional[AbstractInterpreter] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize the PhaseModule with essential components.

        Args:
            output_adapter (OutputModule): The OutputAdapter used to
                          transmit data.
            term_builder (Callable[..., str]): A function from the
                          metadata_manager to construct the action
                          term for each phase.
            metadata_manager (MetadataManager): Manages metadata for
                          the phase.
            interpreter (Optional[AbstractInterpreter]): Optional, an
                          interpreter to process data if needed.
            error_holder (Optional[ErrorHolder]): Optional, an error
                          holder to manage phase errors.
        """
        self._interpreter = interpreter
        self._term_builder = term_builder
        self._metadata_manager = metadata_manager
        self._error_holder = error_holder

    def get_term(self):
        return self._term_builder()
    
    def is_activated(self,topic):
        return topic == self._term_builder
    
    def set_interpreter(self, interpreter: AbstractInterpreter) -> None:
        """
        Set or update the interpreter for the phase.

        Args:
            interpreter (AbstractInterpreter): The interpreter to be
                          used for processing data.
        """
        self._interpreter = interpreter

    def update(self,data=None,**kwargs: Any) -> None:
        """
        Builds the topic that is specifically bound to this phase.

        Args:
            **kwargs (Any): Additional arguments to build the action
                            term.
        """
        return [(self._term_builder(**kwargs),data)]

    def set_error_holder(self, error_holder: ErrorHolder) -> None:
        """
        Set or update the error holder to manage exceptions in this
        phase.

        Args:
            error_holder (ErrorHolder): The error holder for errors
                                        in the phase.
        """
        self._error_holder = error_holder

    def set_metadata_manager(self,manager):
        self._metadata_manager = manager
        if isinstance(self._term_builder,str):
            self._term_builder = eval(f'self._{self._term_builder}')
        
    def _handle_exception(self, exception: Exception) -> None:
        """
        Handle exceptions by passing them to the error holder or
        raising them.

        Args:
            exception (Exception): The exception to handle.
        """
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception
