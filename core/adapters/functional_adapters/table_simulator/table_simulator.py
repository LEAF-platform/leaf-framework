import gzip
import logging
import os
import time
from datetime import datetime, timedelta, date
from threading import Thread
from typing import Any, Optional, List

import dateparser

from core.adapters.equipment_adapter import EquipmentAdapter
from core.adapters.functional_adapters.table_simulator.table_simulator_interpreter import (
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


class TableSimulatorAdapter(EquipmentAdapter):
    def __init__(
        self,
        instance_data,
        output,
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
        watcher: CSVWatcher = CSVWatcher(write_file, metadata_manager)
        measurements = {"experiment": {"measurement": "Aeration rate(Fg:L/h)"}}
        # measurements: list[str] = ["Aeration rate(Fg:L/h)"]
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)

        measure_p: MeasurePhase = MeasurePhase(
            output, metadata_manager, metadata_manager
        )

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
        super().__init__(instance_data=instance_data, watcher=watcher, process_adapters=mock_process, interpreter=TableSimulatorInterpreter(), metadata_manager=metadata_manager)  # type: ignore
        # instance_data,watcher,mock_process,
        #                 interpreter,metadata_manager=metadata_manager)
        self._write_file = write_file
        if start_date is not None:
            self._start_datetime = datetime.combine(start_date, datetime.min.time())
        self._metadata_manager.add_equipment_data(metadata_fn)

    def measurement(self, data: list[str]) -> None:
        logger.info(f"Measurement {data}")

    def metadata(self, data: str) -> None:
        logger.info(f"Metadata {data}")

    def smart_open(self, filepath: str, mode: str = "r") -> Any:
        """Opens the file with gzip if it ends in .gz, otherwise opens normally."""
        if filepath.endswith(".gz"):
            return gzip.open(filepath, mode)
        else:
            return open(filepath, mode)

    def simulate(self, filepath: str, delay: int = 0, wait: int = 0) -> None:
        logger.info(
            f"Simulating nothing yet for {self.instance_id} at {self.institute} with input file {filepath} and wait {wait} and delay {delay}"
        )
        proxy_thread = Thread(target=self.start)
        proxy_thread.start()
        global SEPARATOR
        # Read the big file and push the data to the "file watcher"?
        global SEPARATOR
        with self.smart_open(filepath, "rb") as f:
            for index, lineb in enumerate(f):
                try:
                    line_split: List[str] = (
                        lineb.decode("utf-8").strip().split(SEPARATOR)
                    )
                    if index == 0:
                        header: List[str] = line_split
                        # Rename the time column to timestamp
                        header[header.index(self.time_column)] = "timestamp"
                        lineb = f"{SEPARATOR}".join(header).encode("utf-8")
                        # Checks if file exists and remove if needed
                        if os.path.isfile(self._write_file):
                            logger.warning(
                                f"Trying to run test when the file exists at {self._write_file}"
                            )
                            # Remove the file
                            os.remove(self._write_file)

                    # Change the time column to a datetime object with the start date
                    if index > 0:
                        # Get the time
                        try:
                            time_index = header.index("timestamp")
                            logger.debug(f"Time index: {time_index}")
                        except:
                            raise BaseException(
                                f"Time column 'timestamp' not found in the header {header}"
                            )
                        time_value = line_split[time_index].strip("\"' ")
                        # Check if the time value is a float / integer
                        try:
                            time_h = float(time_value)
                            # Check if self._start_datetime is defined
                            if hasattr(self, "_start_datetime"):
                                # Convert to a datetime object
                                new_time = self._start_datetime + timedelta(
                                    hours=time_h
                                )
                                # Replace the time column
                                line_split[time_index] = new_time.strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                logger.debug(
                                    f"Time value: {time_value} and new time: {line_split[time_index]}"
                                )
                            else:
                                logger.warning(
                                    f"Start date not defined, using {line_split[time_index]} as the time"
                                )
                        except ValueError:
                            logger.debug(f"Time value: {time_value} was not a digit")
                            # Check if the time value is a datetime object
                            time_obj = dateparser.parse(time_value)
                            # Replace the time column
                            line_split[time_index] = time_obj.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            logger.debug(
                                f"Time value: {time_value} and time object: {time_obj} and new time: {line_split[time_index]}"
                            )
                    # Join the line
                    lineb = f"{SEPARATOR}".join(line_split).encode("utf-8") + b"\n"
                    # Write to
                    with open(self._write_file, "ab") as write_file:
                        write_file.write(lineb)
                        logger.info(f"Writing line {index} to {self._write_file}")
                except Exception as e:
                    logger.error(f"Error in simulate: {e}")
                time.sleep(delay)
        # Finish the simulation
        logger.info("Finished simulation")
        self.stop()

    def stop(self) -> None:
        print("Stopping TableSimulatorAdapter")
