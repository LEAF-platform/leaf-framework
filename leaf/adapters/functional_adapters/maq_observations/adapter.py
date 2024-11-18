import logging
import os
from typing import Optional

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.adapters.functional_adapters.maq_observations.interpreter import MAQInterpreter
from leaf.modules.input_modules.simple_watcher import SimpleWatcher
from leaf.metadata_manager.metadata import MetadataManager
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MAQAdapter(StartStopAdapter):
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
        watcher: PollingWatcher = SimpleWatcher(metadata_manager=metadata_manager, interval=10)
        interpreter=MAQInterpreter(token=token),
        super().__init__(instance_data,watcher,output,interpreter,
                         error_holder=error_holder,
                         metadata_manager=metadata_manager)
        self._metadata_manager.add_equipment_data(metadata_fn)

    def _fetch_data(self):
        logger.info("Fetching data????????????????????????????/")