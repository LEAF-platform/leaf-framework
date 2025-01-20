import os
from threading import Thread
import time
from typing import Optional

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.modules.input_modules.csv_watcher import CSVWatcher
from leaf_register.metadata import MetadataManager
from leaf.error_handler.exceptions import AdapterLogicError, SeverityLevel
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.output_modules.output_module import OutputModule

from tests.mock_functional_adapter.interpreter import MockInterpreter
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "device.json")


class MockFunctionalAdapter(StartStopAdapter):
    def __init__(
        self,
        instance_data: dict,
        output: OutputModule,
        write_file: Optional[str] = None,
        maximum_message_size: Optional[int] = 1,
        error_holder: Optional[ErrorHolder] = None,
        experiment_timeout: Optional[int] = None,
    ):
        metadata_manager = MetadataManager()
        watcher = CSVWatcher(write_file, metadata_manager)
        interpreter = MockInterpreter(error_holder=error_holder)

        super().__init__(
            instance_data,
            watcher,
            output,
            interpreter,
            maximum_message_size=maximum_message_size,
            error_holder=error_holder,
            metadata_manager=metadata_manager,
            experiment_timeout=experiment_timeout,
        )
        self._write_file: Optional[str] = write_file
        self._metadata_manager.add_equipment_data(metadata_fn)
