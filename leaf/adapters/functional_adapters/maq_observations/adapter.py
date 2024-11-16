import logging
import os
from typing import Optional

from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.functional_adapters.maq_observations.interpreter import MAQInterpreter
from leaf.modules.input_modules.simple_watcher import SimpleWatcher
from leaf.metadata_manager.metadata import MetadataManager
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.process_modules.discrete_module import DiscreteProcess
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MAQAdapter(EquipmentAdapter):
    def __init__(
        self,
        instance_data,
        output,
        write_file: Optional[str],
        token: str,
        endpoint: str = "https://www.maq-observations.nl",
        error_holder: Optional[ErrorHolder] = None) -> None:
        logger.info(
            f"Initializing MAQ Observations with instance data {instance_data} and output {output} and write file {write_file}"
        )
        # Set variables
        self._endpoint = endpoint
        self._token = token
        # Obtain device metadata
        current_dir = os.path.dirname(os.path.abspath(__file__))
        metadata_fn = os.path.join(current_dir, 'device.json') # Check what can be obtained through the API

        # Create a metadata manager
        metadata_manager: MetadataManager = MetadataManager()
        metadata_manager.load_from_file(metadata_fn)
        # Create a polling watcher
        watcher: PollingWatcher = SimpleWatcher(metadata_manager=metadata_manager, interval=10, measurement_callbacks=[])
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)
        measure_p: MeasurePhase = MeasurePhase(output_adapter=output, metadata_manager=metadata_manager)
        details_p: InitialisationPhase = InitialisationPhase(output, metadata_manager)
        logger.info(f"Instance data: {instance_data}")
        # watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        # watcher.add_stop_callback(stop_p.update)
        # watcher.add_initialise_callback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        mock_process = [DiscreteProcess(phase)]
        super().__init__(instance_data=instance_data, watcher=watcher, 
                         process_adapters=mock_process, 
                         interpreter=MAQInterpreter(token=token), 
                         metadata_manager=metadata_manager,
                         error_holder=error_holder)  # type: ignore
        self._metadata_manager.add_equipment_data(metadata_fn)

    def _fetch_data(self):
        logger.info("Fetching data????????????????????????????/")