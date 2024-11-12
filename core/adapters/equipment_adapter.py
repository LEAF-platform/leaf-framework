import logging
import os
import time
from threading import Event
from abc import ABC, abstractmethod
from typing import List, Optional, Union, Callable, Any
from core.metadata_manager.metadata import MetadataManager
from core.error_handler.exceptions import LEAFError
from core.error_handler import exceptions
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.output_modules.output_module import OutputModule
from core.modules.process_modules.process_module import ProcessModule
from core.error_handler.error_holder import ErrorHolder
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "equipment_adapter.json")

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)


class AbstractInterpreter(ABC):
    """
    Abstract base class for interpreters.

    One interpreter is needed for each EquipmentAdapter class.
    """

    def __init__(self, error_holder: Optional[ErrorHolder] = None):
        """
        Initialize the abstract interpreter with predefined keys for
        measurement and metadate outputs.
        """
        self.id: str = "undefined"
        self.TIMESTAMP_KEY: str = "timestamp"
        self.EXPERIMENT_ID_KEY: str = "experiment_id"
        self.MEASUREMENT_HEADING_KEY: str = "measurement_types"
        self._error_holder: Optional[ErrorHolder] = error_holder

    def set_error_holder(self, error_holder: Optional[ErrorHolder]) -> None:
        self._error_holder = error_holder

    @abstractmethod
    def metadata(self, data: Any) -> None:
        """
        Abstract method to process metadata.

        Args:
            data (Any): The metadata from the InputModule.
        """
        pass

    @abstractmethod
    def measurement(self, data: Any) -> None:
        """
        Abstract method to process measurement data.

        Args:
            data (Any): The measurement from the InputModule.
        """
        pass

    @abstractmethod
    def simulate(self) -> None:
        """
        Abstract method to simulate a finite
        run of equipment using existing data.
        """
        pass

    def _handle_exception(self, exception: Exception) -> None:
        if self._error_holder is not None:
            self._error_holder.add_error(exception)
        else:
            raise exception
        
class EquipmentAdapter(ABC):
    """
    Base class for all equipment adapters. Ensures all derived classes have
    the required composite modules and implements a start and stop function.
    """

    def __init__(self, 
                 instance_data: dict, 
                 watcher: OutputModule, 
                 process_adapters: Union[ProcessModule, List[ProcessModule]], 
                 interpreter: AbstractInterpreter, 
                 metadata_manager: Optional[MetadataManager] = None, 
                 error_holder: Optional[ErrorHolder] = None):
        """
        Initialize the EquipmentAdapter instance.
        
        Args:
            instance_data (dict): Data related to the equipment instance.
            watcher (OutputModule): An object that watches or monitors events or data.
            process_adapters (Union[ProcessModule, List[ProcessModule]]): A list or a single instance of ProcessModules.
            interpreter (AbstractInterpreter): An interpreter object to process the data.
            metadata_manager (Optional[MetadataManager]): Optional metadata manager instance
                              (defaults to new MetadataManager if None).
            error_holder (Optional[ErrorHolder]): Optional error holder instance.
        """
        if not isinstance(process_adapters, (list, tuple, set)):
            process_adapters = [process_adapters]

        logger.debug(f"Process adapters {process_adapters}")
        self._processes: List['ProcessModule'] = process_adapters
        logger.debug(f"Processes {self._processes}")

        [p.set_interpreter(interpreter) for p in self._processes]

        self._interpreter: AbstractInterpreter = interpreter
        self._watcher: OutputModule = watcher
        self._stop_event: Event = Event()

        if metadata_manager is None:
            self._metadata_manager: MetadataManager = MetadataManager()
        else:
            self._metadata_manager = metadata_manager

        self._metadata_manager.load_from_file(metadata_fn)
        self._metadata_manager.add_equipment_data(instance_data)

        self._error_holder: Optional[ErrorHolder] = error_holder
        interpreter.set_error_holder(error_holder)
        watcher.set_error_holder(error_holder)
        [p.set_error_holder(error_holder) for p in self._processes]

    def start(self) -> None:
        """
        Start the equipment adapter process. 
        Use custom exception handling with severity levels.
        """
        self._stop_event.clear()
        if not self._metadata_manager.is_valid():
            logger.error(f"{self._metadata_manager.get_instance_id()} is missing data.")
            return self.stop()

        try:
            self._watcher.start()
            while not self._stop_event.is_set():
                time.sleep(1)
                if self._error_holder is None:
                    continue
                for error, tb in self._error_holder.get_unseen_errors():
                    if not isinstance(error, LEAFError):
                        logger.error(
                            f"{error} added to error holder, only LEAF errors should be used.",
                            exc_info=error)
                        return self.stop()

                    if error.severity == exceptions.SeverityLevel.CRITICAL:
                        logger.error(
                            f"Critical error, shutting down this adapter: {error}",
                            exc_info=error,
                        )
                        self._stop_event.set()
                        self.stop()
                    elif error.severity == exceptions.SeverityLevel.ERROR:
                        logger.error(
                            f"Error, resetting this adapter: {error}", exc_info=error
                        )
                        self.stop()
                        return self.start()

                    elif error.severity == exceptions.SeverityLevel.WARNING:
                        if isinstance(error, exceptions.InputError):
                            logger.warning(
                                f"Warning Input error, taking action on this adapter: {error}",
                                exc_info=error)
                            if self._watcher.is_running():
                                self._watcher.stop()
                            self._watcher.start()
                        elif isinstance(error, exceptions.HardwareStalledError):
                            logger.warning(
                                f"Warning Hardware error, taking action on this adapter: {error}",
                                exc_info=error)
                            # Not much can be done here
                            raise NotImplementedError()
                        elif isinstance(error, exceptions.ClientUnreachableError):
                            # This should generally not occur.
                            logger.warning(
                                f"Warning Client error, taking action on this adapter: {error}",
                                exc_info=error)
                            if error.client is not None:
                                error.client.stop()
                                time.sleep(1)
                                error.client.start()
                        elif isinstance(error, exceptions.AdapterLogicError):
                            # Adapter logic errors may need granular handling
                            pass
                        elif isinstance(error, exceptions.InterpreterError):
                            # Interpreter errors typically indicate data or implementation issues
                            pass
                    elif error.severity == exceptions.SeverityLevel.INFO:
                        logger.info(
                            f"Information error, no action needed: {error}", exc_info=error)

        except KeyboardInterrupt:
            logger.info("User keyboard input stopping adapter.")
            self._stop_event.set()
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            self._stop_event.set()
        finally:
            logger.info("Stopping the watcher and cleaning up.")
            self._watcher.stop()
            self.stop()

    def stop(self) -> None:
        """
        Stop the equipment adapter process.

        Stops the watcher and flushes all output channels.
        """
        for process in self._processes:
            for phase in process._phases:
                phase._output.flush(self._metadata_manager.details())
                phase._output.flush(self._metadata_manager.running())
                phase._output.flush(self._metadata_manager.experiment.start())
                phase._output.flush(self._metadata_manager.experiment.stop())
        self._stop_event.set()
        if self._watcher.is_running():
            self._watcher.stop()


