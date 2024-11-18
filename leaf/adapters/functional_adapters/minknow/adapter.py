import logging
import os
from typing import Optional

from minknow_api.manager import Manager

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.adapters.functional_adapters.minknow.interpreter import MinKNOWInterpreter
from leaf.metadata_manager.metadata import MetadataManager
from leaf.modules.input_modules.simple_watcher import SimpleWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MinKNOWAdapter(StartStopAdapter):
    def __init__(
        self,
        instance_data,
        output,
        write_file: Optional[str],
        token: Optional[str],
        host: str = "localhost",
        port: int = 9501,
        error_holder: Optional[ErrorHolder] = None) -> None:
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
        super().__init__(instance_data,watcher,output,interpreter,
                         error_holder=error_holder,
                         metadata_manager=metadata_manager)
        self._write_file = write_file
        self._metadata_manager.add_equipment_data(metadata_fn)

    def stop(self) -> None:
        logger.debug("Stopping the minKNOW adapter")
