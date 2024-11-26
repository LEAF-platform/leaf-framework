import logging
import time
from typing import Optional, Any

from influxobject import InfluxPoint

from leaf.modules.phase_modules.phase import PhaseModule
from leaf.modules.output_modules.output_module import OutputModule
from leaf.metadata_manager.metadata import MetadataManager
from leaf.error_handler.exceptions import AdapterLogicError
from leaf.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MeasurePhase(PhaseModule):
    """
    Handles the measurement-related actions within a process.
    It transmits measurement data.
    """

    def __init__(self, 
                 output_adapter: OutputModule, 
                 metadata_manager: MetadataManager, 
                 maximum_message_size: Optional[int] = 1) -> None:
        """
        Initialise the MeasurePhase with the output adapter,
        metadata manager, and optional maximum_message_size transmission 
        setting.

        Args:
            output_adapter (OutputModule): The OutputModule used 
                           to transmit data.
            metadata_manager (MetadataManager): Manages metadata 
                             associated with the phase.
            maximum_message_size (bool): The maximum number of measurements 
                                          in a single message.
        """
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter, term_builder, metadata_manager)
        self._maximum_message_size: int = maximum_message_size

    def update(self, data: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Called by the InputModule, uses interpreter to get the new
        measurements and transmits the data using the OutputModule.

        Args:
            data (Optional[Any]): Optional data to be transmitted.
            **kwargs (Any): Additional arguments used to build the 
                     action term.
        """
        if data is None:
            excp = AdapterLogicError("Measurement system activated without any data")
            self._handle_exception(excp)
            return

        if self._interpreter is not None:
            # Check if attributes are set
            if getattr(self._interpreter, 'id', None) is None:
                self._interpreter.id = "invalid_interpreter_id"
                logger.error(f'No ID found for interpreter: {self._interpreter}')
            exp_id = self._interpreter.id or "invalid_interpreter_id"
            if exp_id is None:
                excp = AdapterLogicError(
                    "Trying to transmit "
                    "measurements outside of "
                    "experiment (No experiment id)"
                )
                self._handle_exception(excp)

            result = self._interpreter.measurement(data)
            if result is None:
                excp = AdapterLogicError(
                    "Interpreter couldn't parse measurement, likely metadata has been "
                    "provided as measurement data."
                )
                self._handle_exception(excp)
                return
            if isinstance(result,(set,list,tuple)):
                chunks = [result[i:i + self._maximum_message_size] for 
                          i in range(0, len(result), self._maximum_message_size)]
                for chunk in chunks:
                    self._transmit_message(exp_id,chunk)
                    time.sleep(0.1)
            else:
                self._transmit_message(exp_id,result)
        else:
            super().update(data, **kwargs)


    def _transmit_message(self,experiment_id,result):
        measurement = "unknown"
        if isinstance(result,dict):
            measurement = result["measurement"]
        elif isinstance(result,InfluxPoint):
            result = result.to_json()
            measurement = result["measurement"]
        elif isinstance(result,list):
            result = [l.to_json() if isinstance(l, InfluxPoint) else l for l in result]
        else:
            logger.error(f"Unknown measurement data type: {type(result)}")
        
        action = self._term_builder(experiment_id=experiment_id, 
                                    measurement=measurement)
        return self._output.transmit(action, result)
    
        