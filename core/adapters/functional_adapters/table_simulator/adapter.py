import gzip
import logging
import os
from datetime import datetime, date
from typing import Any, Optional

from core.adapters.equipment_adapter import EquipmentAdapter
from core.adapters.functional_adapters.table_simulator.interpreter import (
    TableSimulatorInterpreter,
)
from core.metadata_manager.metadata import MetadataManager
from core.modules.input_modules.csv_watcher import CSVWatcher
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.phase_modules.initialisation import InitialisationPhase
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.process_modules.discrete_module import DiscreteProcess

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "table_simulator.json")

SEPARATOR: str = ","


def smart_open(filepath: str, mode: str = "r") -> Any:
    """Opens the file with gzip if it ends in .gz, otherwise opens normally."""
    if filepath.endswith(".gz"):
        return gzip.open(filepath, mode)
    else:
        return open(filepath, mode)


class TableSimulatorAdapter(EquipmentAdapter):
    def __init__(
        self,
        instance_data: dict[str, str],
        output: Any, # Any of the output_modules?
        write_file: Optional[str],
        time_column: str,
        start_date: Optional[date] = None,
        sep: str = ",",
    ) -> None:
        logger.info(
            f"Initializing TableSimulator with instance data {instance_data} and output {output} and write file {write_file}"
        )
        metadata_manager: MetadataManager = MetadataManager()
        metadata_manager.add_metadata("sep", sep)
        metadata_manager.add_metadata("experiment", "experiment?")
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(file_path=write_file, metadata_manager=metadata_manager, measurement_callbacks=[self.measurement])
        logger.info(f"Watcher set: {watcher}")
        # measurements = {"experiment": {"measurement": "Aeration rate(Fg:L/h)"}}
        # measurements: list[str] = ["Aeration rate(Fg:L/h)"]
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)

        measure_p: MeasurePhase = MeasurePhase(output_adapter=output, 
                                               metadata_manager=metadata_manager)

        details_p: InitialisationPhase = InitialisationPhase(output, metadata_manager)
        self.instance_id: str = instance_data["instance_id"]
        self.institute: str = instance_data["institute"]
        self.time_column: str = time_column
        global SEPARATOR
        SEPARATOR = sep
        # Obtain absolute path to the input file
        # if input_file is not None:
        #     self.input_file = os.path.abspath(input_file)
        logger.info(f"Instance data: {instance_data}")
        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        mock_process = [DiscreteProcess(phase)]
        super().__init__(instance_data=instance_data, watcher=watcher, process_adapters=mock_process, interpreter=TableSimulatorInterpreter(time_column, start_date, sep), metadata_manager=metadata_manager)  # type: ignore
        self._write_file = write_file
        if start_date is not None:
            self._start_datetime = datetime.combine(start_date, datetime.min.time())
        self._metadata_manager.add_equipment_data(metadata_fn)

    def measurement(self, data: list[str]) -> None:
        logger.debug(f"Measurement detected with {len(data)} rows")

    def metadata(self, data: str) -> None:
        logger.info(f"Metadata {data}")

    def stop(self) -> None:
        """
        Stop the equipment adapter process.

        Stops the watcher and flushes all output channels.
        """
        logger.info("Stopping TableSimulatorAdapter")
        # Needs reworking really.
        for process in self._processes:
            for phase in process._phases:
                # Flush all retained mqtt topics.
                logger.info(f"Flushing {phase._output}")
                phase._output.flush(self._metadata_manager.details())
                phase._output.flush(self._metadata_manager.running())
                phase._output.flush(self._metadata_manager.experiment.start())
                phase._output.flush(self._metadata_manager.experiment.start())
        self._stop_event.set()
        self._watcher.stop()
