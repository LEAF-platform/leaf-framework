import logging
import time
from typing import Optional, Any

import influxobject

from core.modules.phase_modules.phase import PhaseModule
from core.modules.output_modules.output_module import OutputModule
from core.metadata_manager.metadata import MetadataManager
from core.error_handler.exceptions import AdapterLogicError
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MeasurePhase(PhaseModule):
    """
    Handles the measurement-related actions within a process.
    It transmits measurement data and can stagger 
    transmission if needed.
    """

    def __init__(self, 
                 output_adapter: OutputModule, 
                 metadata_manager: MetadataManager, 
                 stagger_transmit: bool = False) -> None:
        """
        Initialise the MeasurePhase with the output adapter,
        metadata manager, and optional stagger transmission setting.

        Args:
            output_adapter (OutputModule): The OutputModule used 
                           to transmit data.
            metadata_manager (MetadataManager): Manages metadata 
                             associated with the phase.
            stagger_transmit (bool): Whether to stagger the 
                             transmission of measurements.
        """
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter, term_builder, metadata_manager)
        self._stagger_transmit: bool = stagger_transmit

    def update(self, data: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Called by the InputModule, uses interpreter to get the new
        measurements and transmits the data using the OutputModule.
        If staggered transmission is enabled, data is 
        transmitted piece by piece.

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
            logger.debug(f"Interpreter: {self._interpreter}")
            logger.debug(f"Interpreter dict: {self._interpreter.__dict__}")
            logger.debug(f"Interpreter ID: {self._interpreter.id}")
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

            if isinstance(result, dict):
                # action = self._term_builder(experiment_id=exp_id, measurement="unknown")
                # if 'measurement' in result:
                #     logger.debug(f"Transmitting measurement: {result['measurement']}")
                #     action = self._term_builder(experiment_id=exp_id, measurement=result['measurement'])
                # self._output.transmit(action, result)
                if self._stagger_transmit:
                    logger.debug(f"Transmitting measurements as stagger: {result}")
                    for measurement_type, measurements in result["fields"].items():
                        if not isinstance(measurements, list):
                            measurements = [measurements]
                        for measurement in measurements:
                            action = self._term_builder(
                                experiment_id=exp_id, measurement=measurement_type
                            )
                            if isinstance(measurement, dict):
                                measurement["timestamp"] = result["timestamp"]
                            self._output.transmit(action, measurement)
                else:
                    action = self._term_builder(
                        experiment_id=exp_id, measurement=result["measurement"]
                    )
                    self._output.transmit(action, result)
            elif isinstance(result, str):
                logger.debug(f"Transmitting measurement as string: {result}")
                action = self._term_builder(experiment_id=exp_id, measurement="unknown")
                self._output.transmit(action, result)
            elif isinstance(result, influxobject.influxpoint.InfluxPoint):
                logger.debug(f"Transmitting measurement as InfluxPoint: {result}")
                result = result.to_json()
                action = self._term_builder(experiment_id=exp_id, measurement=result["measurement"])
                self._output.transmit(action, result)
            elif isinstance(result, list):
                logger.debug(f"Transmitting measurements as list: {len(result)}")
                for index, measurement in enumerate(result):
                    measurement = measurement.to_json()
                    action = self._term_builder(experiment_id=exp_id, measurement=measurement["measurement"])
                    self._output.transmit(action, measurement)
                    time.sleep(0.1)
                    if index % 10 == 0:
                        logger.debug(f"Transmitted {index} measurements of {len(result)}")
            else:
                logger.error(f"Unknown measurement data type: {type(result)}")
                action = self._term_builder(experiment_id=exp_id, measurement="unknown")
                self._output.transmit(action, result)
        else:
            super().update(data, **kwargs)
