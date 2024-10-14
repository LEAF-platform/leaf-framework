import gzip
import logging
import os
import time
from datetime import datetime, timedelta
import uuid
from threading import Thread
from typing import Dict, Union, Any

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


current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "indpensim.json")


class IndPenSimInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        logger.info("Initializing IndPenSimInterpreter")

    def metadata(self, data: str) -> dict[str, str]:
        self.id = f"{str(uuid.uuid4())}"
        payload = {
            self.TIMESTAMP_KEY: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            self.EXPERIMENT_ID_KEY: self.id,
        }
        return payload
    
    def measurement(self, data: list[str]) -> Dict[str, Union[str, Dict[str, str], Dict[str, Union[int, float, str]], str]]:
        logger.info(f"data {str(data)[:50]}...")
        # Load measurement into a pd
        # List of lists to DataFrame where the first row is the header
        for index, x in enumerate(data):
            data[index] = x[0].split(",")
        # TODO Load only the last row
        df = pd.DataFrame(data)
        logger.debug(f"Dimensions of the data: {df.shape}")
        # Check if there are enough rows
        if df.shape[0] < 2:  # Not enough rows
            return {}
        # Set the first row as the header
        df.columns = df.iloc[0]
        # Get the last row
        last_row = df.iloc[-1]
        # Get the last row as a dictionary with the column names as keys
        last_row_dict = last_row.to_dict()
        # Remove all numeric keys
        for key in list(last_row_dict.keys()):
            # Remove all keys that are numbers
            if key.isdigit():
                del last_row_dict[key]
            # Remove empty entries
            elif last_row_dict[key] == "":
                del last_row_dict[key]
            # Check if it can be converted to a float
            else:
                try:
                    # Convert to float
                    last_row_dict[key] = float(last_row_dict[key])
                    # If integer, convert to int
                    if last_row_dict[key].is_integer():
                        last_row_dict[key] = int(last_row_dict[key])
                except ValueError:
                    pass
            # Replace the key with a cleaned version
            if key in last_row_dict:
                new_key = key.split("(")[0].strip().replace(" ", "_")
                last_row_dict[new_key] = last_row_dict.pop(key)
        # Create the influx point object for a final message
        influx_point = InfluxPoint()
        influx_point.set_measurement("indpensim")
        influx_point.set_fields(last_row_dict)
        time_obj = datetime.strptime(last_row_dict["Time"], "%Y-%m-%d %H:%M:%S")
        influx_point.set_timestamp(time_obj)
        influx_point.add_tag("project", "indpensim")
        # Remove time
        influx_point.remove_field("Time")
        # Send message to the MQTT broker
        return influx_point.to_json()

    def simulate(self) -> None:
        logger.error("Simulating IndPenSimInterpreter")
        print("Doing something D?")


class IndPenSimAdapter(EquipmentAdapter):
    def __init__(self, instance_data, output, write_file=None) -> None:
        logger.info(
            f"Initializing IndPenSimAdapter with instance data {instance_data} and output {output} and write file {write_file}"
        )
        metadata_manager: MetadataManager = MetadataManager()
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(write_file, metadata_manager, delimeter=",")
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)
        measure_p: MeasurePhase = MeasurePhase(output, metadata_manager)
        details_p: InitialisationPhase = InitialisationPhase(output, metadata_manager)
        logger.info(f"Instance data: {instance_data}")
        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_calmeasurementlback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        process = [DiscreteProcess(phase)]
        super().__init__(
            instance_data=instance_data,
            watcher=watcher,
            process_adapters=process,
            interpreter=IndPenSimInterpreter(),
            metadata_manager=metadata_manager,
        )
        self._write_file = write_file
        self._metadata_manager.add_equipment_data(metadata_fn)

    def simulate(self, filepath: str, wait: int = 0, delay: int = 0) -> None:
        logger.info(
            f"Simulating nothing yet for {self.instance_id} at {self.institute} with input file {filepath} and wait {wait} and delay {delay}"
        )
        proxy_thread = Thread(target=self.start)
        proxy_thread.start()

        # Read the big file and push the data to the "file watcher"?
        with gzip.open(filepath, "r") as f:
            for index, lineb in enumerate(f):
                if index == 0:
                    if os.path.isfile(self._write_file):
                        logger.warning(
                            f"Trying to run test when the file exists at {self._write_file}"
                        )
                        # Remove the file
                        os.remove(self._write_file)
                line: str = lineb.decode("utf-8")
                # Change the time column to a datetime object with the start date
                if index > 0:
                    line_split = line.split(",")
                    # Get the time
                    time_h = float(line_split[0])
                    # Convert to a datetime object
                    new_time = self._start_datetime + timedelta(hours=time_h)
                    # Replace the time column
                    line_split[0] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                    line = ",".join(line_split)

                with open(self._write_file, "a") as write_file:
                    write_file.write(line)
                    logger.info(f"Writing line {index} to {self._write_file}")
                time.sleep(wait)

    def stop(self) -> None:
        print("Stopping IndPenSimAdapter")
