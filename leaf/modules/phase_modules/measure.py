import logging
import time
from typing import Optional, Any

from influxobject import InfluxPoint

from leaf.modules.phase_modules.phase import PhaseModule
from leaf.error_handler.exceptions import AdapterLogicError

class MeasurePhase(PhaseModule):
    """
    Handles the measurement-related actions within a process.
    It transmits measurement data.
    """

    def __init__(self, metadata_manager = None, 
                 maximum_message_size: Optional[int] = 1,
                 error_holder=None) -> None:
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
        if metadata_manager is not None:
            term_builder = metadata_manager.experiment.measurement
        else:
            term_builder = "metadata_manager.experiment.measurement"
            
        super().__init__(term_builder, 
                         metadata_manager=metadata_manager,
                         error_holder=error_holder)
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
            return None

        if self._interpreter is not None:
            # Check if attributes are set
            if getattr(self._interpreter, 'id', None) is None:
                self._interpreter.id = "invalid_id"
            exp_id = self._interpreter.id
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
                return None
            if isinstance(result,(set,list,tuple)):
                chunks = [result[i:i + self._maximum_message_size] for 
                          i in range(0, len(result), self._maximum_message_size)]
                messages = []
                for chunk in chunks:
                    messages.append(self._form_message(exp_id,chunk))
                    time.sleep(0.1)
                return messages
            else:
                return [self._form_message(exp_id,result)]
        else:
            if "experiment_id" in kwargs:
                experiment_id= kwargs["experiment_id"]
            else:
                experiment_id="unknown"
            if "measurement" in kwargs:
                measurement= kwargs["measurement"]
            else:
                measurement="unknown"

            action = self._term_builder(experiment_id=experiment_id, 
                                        measurement=measurement)
            return [(action,data)]


    def _form_message(self,experiment_id,result):
        measurement = "unknown"
        if isinstance(result,dict):
            measurement = result["measurement"]
        elif isinstance(result,InfluxPoint):
            result = result.to_json()
            measurement = result["measurement"]
        elif isinstance(result,list):
            result = [l.to_json() if isinstance(l, InfluxPoint) 
                      else l for l in result]
        else:
            excp = AdapterLogicError(f"Unknown measurement data type: {type(result)}")
            self._handle_exception(excp)
        
        action = self._term_builder(experiment_id=experiment_id, 
                                    measurement=measurement)
        return (action, result)
    
        