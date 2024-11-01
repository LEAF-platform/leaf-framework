import logging
import os
from typing import Optional

from minknow_api.manager import Manager

from core.adapters.equipment_adapter import EquipmentAdapter
from core.adapters.functional_adapters.minknow.interpreter import MinKNOWInterpreter
from core.metadata_manager.metadata import MetadataManager
from core.modules.input_modules.simple_watcher import SimpleWatcher
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.phase_modules.initialisation import InitialisationPhase
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.process_modules.discrete_module import DiscreteProcess

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MinKNOWAdapter(EquipmentAdapter):
    def __init__(
        self,
        instance_data,
        output,
        write_file: Optional[str],
        token: Optional[str],
        host: str = "localhost",
        port: int = 9501,
    ) -> None:
        logger.info(
            f"Initializing TableSimulator with instance data {instance_data} and output {output} and write file {write_file}"
        )
        # Set variables
        self._host = host
        self._port = port
        self._token = token
        self._manager = Manager(
            host=self._host,
            port=self._port,
            developer_api_token=self._token,
        )
        # Obtain device metadata
        current_dir = os.path.dirname(os.path.abspath(__file__))
        metadata_fn = os.path.join(current_dir, 'minknow.json') # Check what can be obtained through the API

        # Create a metadata manager
        metadata_manager: MetadataManager = MetadataManager()
        metadata_manager.add_metadata("a", "b")
        # Create a CSV watcher for the write file
        watcher: SimpleWatcher = SimpleWatcher(metadata_manager=metadata_manager, interval=10, measurement_callbacks=MinKNOWInterpreter.measurement)
        measurements: list[str] = ["Aeration rate(Fg:L/h)"]
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)
        measure_p: MeasurePhase = MeasurePhase(output_adapter=output, metadata_manager=metadata_manager)
        details_p: InitialisationPhase = InitialisationPhase(output, metadata_manager)
        logger.info(f"Instance data: {instance_data}")
        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        mock_process = [DiscreteProcess(phase)]
        super().__init__(instance_data=instance_data, watcher=watcher, process_adapters=mock_process, interpreter=interpreter, metadata_manager=metadata_manager)  # type: ignore
        self._write_file = write_file
        self._metadata_manager.add_equipment_data(metadata_fn)

    def stop(self) -> None:
        logger.debug("Stopping the minKNOW adapter")
