import gzip
import logging
import math
import os
import time
from datetime import datetime, timedelta, date
from pprint import pprint
from threading import Thread
from typing import Dict, Union, Any, Optional, List
import dateparser
import pandas as pd
from influxobject import InfluxPoint

from core.adapters.equipment_adapter import AbstractInterpreter, EquipmentAdapter
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


class TableSimulatorInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        logger.info("Initializing TableSimulatorInterpreter")

    def measurement(
        self, data: list[str], measurements: Any
    ) -> Dict[str, Union[str, Dict[str, str], Dict[str, Union[int, float, str]], str]]:
        logger.info(f"TableSimulatoInterpreter data {str(data)[:50]}...")
        # Load measurement into a pd
        # List of lists to DataFrame where the first row is the header
        global SEPARATOR
        matrix = [
            x[0].split(SEPARATOR) for x in data if isinstance(x, list) and len(x) > 0
        ]
        logger.debug(f"Matrix: {len(matrix)}")
        # TODO Load only the last row
        df = pd.DataFrame([matrix[0], matrix[-1]])
        if df.iloc[0].equals(df.iloc[-1]):
            return {}
        # Check if there are enough rows
        if df.shape[0] < 2:
            return {}
        # logger.debug(f"Dimensions of the data: {df.shape}")
        # Set the first row as the header
        df.columns = df.iloc[0]
        # Get the last row
        last_row = df.iloc[-1]
        # Get the last row as a dictionary with the column names as keys
        last_row_dict = last_row.to_dict()
        # Remove all numeric keys
        for key in list(last_row_dict.keys()):
            # No modifications to the timestamp needed
            if key == "timestamp":
                continue
            # Remove all keys that are numbers
            if key.isdigit():
                del last_row_dict[key]
            # Remove empty entries
            elif last_row_dict[key] in ["", "NaN", None] or (
                isinstance(last_row_dict[key], float) and math.isnan(last_row_dict[key])
            ):
                del last_row_dict[key]
            else:
                # Check if it can be converted to a float
                try:
                    # Convert to float
                    last_row_dict[key] = float(last_row_dict[key])
                    # If integer, convert to int
                    if last_row_dict[key].is_integer():
                        last_row_dict[key] = int(last_row_dict[key])
                except ValueError:
                    logger.debug(f"Could not convert {last_row_dict[key]} to a float")
            # Replace the key with a cleaned version
            if key in last_row_dict:
                new_key = key.split("(")[0].strip().replace(" ", "_")
                last_row_dict[new_key] = last_row_dict.pop(key)
        # Create the influx point object for a final message
        influx_point = InfluxPoint()
        influx_point.set_measurement("table_simulator")
        influx_point.set_fields(last_row_dict)
        try:
            # time_obj = datetime.strptime(last_row_dict["timestamp"], '%Y-%m-%d %H:%M:%S')
            time_obj = dateparser.parse(last_row_dict["timestamp"])
            logger.debug(f"Time object: {time_obj}")
            influx_point.set_timestamp(time_obj)
            influx_point.add_tag("project", "table_simulator")
            # Send message to the MQTT broker
            return influx_point.to_json()
        except Exception as e:
            raise BaseException(f"Error in TableSimulatoInterpreter: {e}")

    def metadata(self, data: str) -> dict[str, str]:
        logger.debug(f"Metadata {str(data)[:50]}")
        return {"metadata": "Some content", "experiment_id": "THIS IS WRONG"}

    def simulate(self) -> None:
        logger.error("Simulating TableSimulatorInterpreter")
        print("Doing something D?")


interpreter = TableSimulatorInterpreter()


class TableSimulatorAdapter(EquipmentAdapter):
    def __init__(
        self,
        instance_data: Any,
        output: Any,
        write_file: Optional[str],
        time_column: str,
        start_date: Optional[date] = None,
        sep: str = ",",
    ) -> None:
        logger.info(
            f"Initializing TableSimulator with instance data {instance_data} and output {output} and write file {write_file}"
        )
        metadata_manager: MetadataManager = MetadataManager()
        metadata_manager.set_metadata("sep", sep)
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(write_file, metadata_manager)
        measurements: list[str] = ["Aeration rate(Fg:L/h)"]
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)
        measure_p: MeasurePhase = MeasurePhase(output, measurements, metadata_manager)
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
        super().__init__(instance_data=instance_data, watcher=watcher, process_adapters=mock_process, interpreter=interpreter, metadata_manager=metadata_manager)  # type: ignore
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
