import gzip
import logging
import os
from datetime import datetime, date
from typing import Any, Optional

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.adapters.functional_adapters.table_simulator.interpreter import (
    TableSimulatorInterpreter,
)
from leaf.metadata_manager.metadata import MetadataManager
from leaf.modules.input_modules.csv_watcher import CSVWatcher
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "device.json")

SEPARATOR: str = ","


def smart_open(filepath: str, mode: str = "r") -> Any:
    """Opens the file with gzip if it ends in .gz, otherwise opens normally."""
    if filepath.endswith(".gz"):
        return gzip.open(filepath, mode)
    else:
        return open(filepath, mode)


class TableSimulatorAdapter(StartStopAdapter):
    def __init__(
        self,
        instance_data: dict[str, str],
        output: Any, # Any of the output_modules?
        write_file: Optional[str],
        time_column: str,
        start_date: Optional[date] = None,
        sep: str = ",",
        error_holder: Optional[ErrorHolder] = None
    ) -> None:
        logger.info(
            f"Initializing TableSimulator with instance data {instance_data} and output {output} and write file {write_file}"
        )
        metadata_manager: MetadataManager = MetadataManager()
        metadata_manager.add_metadata("sep", sep)
        metadata_manager.add_metadata("experiment", "experiment?")
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(file_path=write_file, metadata_manager=metadata_manager)
        logger.info(f"Watcher set: {watcher}")
        # measurements = {"experiment": {"measurement": "Aeration rate(Fg:L/h)"}}
        # measurements: list[str] = ["Aeration rate(Fg:L/h)"]

        self.instance_id: str = instance_data["instance_id"]
        self.institute: str = instance_data["institute"]
        self.time_column: str = time_column
        global SEPARATOR
        SEPARATOR = sep
        interpreter = TableSimulatorInterpreter(time_column, start_date, sep)
        super().__init__(instance_data,watcher,output,interpreter,
                         error_holder=error_holder,
                         metadata_manager=metadata_manager)
        
        self._write_file = write_file
        if start_date is not None:
            self._start_datetime = datetime.combine(start_date, datetime.min.time())
        self._metadata_manager.add_equipment_data(metadata_fn)