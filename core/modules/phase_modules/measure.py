import logging

import influxobject

from core.modules.logger_modules.logger_utils import get_logger
from core.modules.phase_modules.phase import PhaseModule

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MeasurePhase(PhaseModule):
    """
    Handles the measurement related actions within a process.
    It transmits measurement data and can stagger transmission if needed.
    """
    
    def __init__(self, output_adapter, metadata_manager, stagger_transmit: bool=False) -> None:
        """
        Initialise the MeasurePhase with the output adapter, 
        metadata manager,and optional stagger transmission setting.

        Args:
            output_adapter: The OutputAdapter used to transmit data.
            metadata_manager: Manages metadata associated with the phase.
            stagger_transmit: Whether to stagger the transmission of measurements.
        """
        term_builder = metadata_manager.experiment.measurement
        super().__init__(output_adapter, term_builder, metadata_manager)
        self._stagger_transmit = stagger_transmit

    def update(self, data=None, **kwargs) -> None:
        """
        Is called by the InputModule, uses interpreter to get the new 
        measurements and transmits the data using the OutputModule.
        If staggered transmission is enabled, data is 
        transmitted piece by piece.

        Args:
            data: Optional data to be transmitted.
            **kwargs: Additional arguments used to build the action term.
        """
        if self._interpreter is not None:
            exp_id = self._interpreter.id
            if exp_id is None:
                raise ValueError(f'Trying to transmit measurements outside of experiment.')

            result = self._interpreter.measurement(data)

            if result is None:
                super().update(data, **kwargs)

            if isinstance(result, dict):
                # action = self._term_builder(experiment_id=exp_id, measurement="unknown")
                # if 'measurement' in result:
                #     logger.debug(f"Transmitting measurement: {result['measurement']}")
                #     action = self._term_builder(experiment_id=exp_id, measurement=result['measurement'])
                # self._output.transmit(action, result)
                if self._stagger_transmit:
                    for measurement_type, measurements in result["fields"].items():
                        if not isinstance(measurements,list):
                            measurements = [measurements]
                        for measurement in measurements:
                            action = self._term_builder(experiment_id=exp_id, measurement=measurement_type)
                            if isinstance(measurement,dict):
                                measurement["timestamp"] = result["timestamp"]
                            self._output.transmit(action, measurement)
                else:
                    action = self._term_builder(experiment_id=exp_id, measurement=result["measurement"])
                    self._output.transmit(action, result)
            elif isinstance(result, str):
                action = self._term_builder(experiment_id=exp_id, measurement="unknown")
                self._output.transmit(action, result)
            elif isinstance(result, influxobject.influxpoint.InfluxPoint):
                result = result.to_json()
                action = self._term_builder(experiment_id=exp_id, measurement=result["measurement"])
                self._output.transmit(action, result)
            else:
                logger.error(f"Unknown measurement data type: {type(result)}")
                action = self._term_builder(experiment_id=exp_id, measurement="unknown")
                self._output.transmit(action, result)
        else:
            super().update(data, **kwargs)
