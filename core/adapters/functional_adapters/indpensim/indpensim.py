import asyncio
import gzip
import json
import logging
import os
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Dict, Any, Union
from influxobject import InfluxPoint

from core.adapters.equipment_adapter import AbstractInterpreter, EquipmentAdapter
from core.metadata_manager.metadata import MetadataManager
from core.modules.input_modules.csv_watcher import CSVWatcher
from core.modules.phase_modules.initialisation import InitialisationPhase
from core.modules.phase_modules.measurement import MeasurementPhase
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.process_modules.discrete_module import DiscreteProcess

from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'indpensim.json')

class IndPenSimInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        logger.info("Initializing IndPenSimInterpreter")
        print("Doing something A?")
    def measurement(self, data: list[str], measurements) -> Dict[str, Union[str, Dict[str, str], Dict[str, Union[int, float, str]], str]]:
        logger.info(f"Measurement {str(data)[:50]}")
        print("Doing something B?")
        # Load measurement into a pd
        import pandas as pd
        # List of lists to DataFrame where the first row is the header
        for index, x in enumerate(data):
            data[index] = x[0].split(",")
        df = pd.DataFrame(data)
        print(f"Dimensions of the data: {df.shape}")
        if df.shape[0] < 2: # Not enough rows
            return {}
        df.columns = df.iloc[0]
        # Get the last row
        last_row = df.iloc[-1]
        # Get the last row as a dictionary with the column names as keys
        last_row_dict = last_row.to_dict()
        # Remove all numeric keys
        for key in list(last_row_dict.keys()):
            if key.isdigit():
                del last_row_dict[key]
            elif last_row_dict[key] == "":
                del last_row_dict[key]
            # Check if it can be converted to a float
            else:
                try:
                    last_row_dict[key] = float(last_row_dict[key])
                    # If integer, convert to int
                    if last_row_dict[key].is_integer():
                        last_row_dict[key] = int(last_row_dict[key])
                except ValueError:
                    pass
            # Replace the key
            if key in last_row_dict:
                new_key = key.split("(")[0].strip().replace(" ", "_")
                last_row_dict[new_key] = last_row_dict.pop(key)
        # Create the influx point object for a final message
        update: Dict[str, float] = {"some": "update"}
        influx_point = InfluxPoint()
        influx_point.set_measurement('indpensim')
        influx_point.set_fields(last_row_dict)
        time_obj = datetime.strptime(last_row_dict["Time"], '%Y-%m-%d %H:%M:%S')
        influx_point.set_timestamp(time_obj)
        influx_point.add_tag('project', 'indpensim')
        print(f"Set time: {influx_point.timestamp}")
        # Remove time
        influx_point.remove_field("Time")
        # Send message to the MQTT broker
        return influx_point.to_json()
    def metadata(self,data: str) -> dict[str, str]:
        logger.debug(f"Metadata {str(data)[:50]}")
        return {"metadata": "Some content"}
    def start(self) -> None:
        logger.debug("Starting IndPenSimInterpreter")
        print("Doing something D?")
    def stop(self) -> None:
        logger.debug("Stopping IndPenSimInterpreter")
        print("Doing something E?")
    def simulate(self) -> None:
        logger.error("Simulating IndPenSimInterpreter")
        print("Doing something D?")


interpreter = IndPenSimInterpreter()

class IndPenSimAdapter(EquipmentAdapter):
    def __init__(self, instance_data, output, start_date: str, write_file=None):
        logger.info(f"Initializing IndPenSimAdapter with instance data {instance_data} and output {output} and write file {write_file}")
        metadata_manager: MetadataManager = MetadataManager()
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(write_file, metadata_manager)
        measurements: list[str] = ["Aeration rate(Fg:L/h)"]
        # Create the phases?
        start_p = StartPhase(output, metadata_manager)
        stop_p = StopPhase(output, metadata_manager)
        measure_p = MeasurementPhase(output, measurements, metadata_manager)
        details_p = InitialisationPhase(output, metadata_manager)
        self.instance_id = instance_data['instance_id']
        self.institute = instance_data['institute']
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
        super().__init__(instance_data=instance_data, watcher=watcher, process_adapters=mock_process,interpreter=interpreter, metadata_manager=metadata_manager)
        #instance_data,watcher,mock_process,
        #                 interpreter,metadata_manager=metadata_manager)
        self._write_file = write_file
        self._start_datetime = datetime.combine(start_date, datetime.min.time())

        self._metadata_manager.add_equipment_data(metadata_fn)

    def measurement(self, data: str) -> None:
        logger.info(f"Measurement {data}")
        interpreter.measurement(data, ["Aeration rate(Fg:L/h)"])

    def metadata(self, data: str) -> None:
        logger.info(f"Metadata {data}")

    # def start(self) -> None:
    #     logger.info("Starting IndPenSimAdapter")
    #     # Start the async simulate task from a synchronous method
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)
    #
    #     # Run the async simulate function in this thread's event loop
    #     logger.info("Running simulate code for IndPenSimAdapter")
    #     loop.run_until_complete(self.simulate())
    #     logger.info("Finished running simulate code for IndPenSimAdapter")
    #     # Sleep forever
    #     while True:
    #         logger.info("ZzzzZZzZzzzZZZZZzZzzz")
    #         time.sleep(10)

    def simulate(self,filepath:str,wait: int=0,delay=None) -> None:
        logger.info(f"Simulating nothing yet for {self.instance_id} at {self.institute} with input file {filepath} and wait {wait} and delay {delay}")
        proxy_thread = Thread(target=self.start)
        proxy_thread.start()

        # Read the big file and push the data to the "file watcher"?
        with gzip.open(filepath, "r") as f:
            for index, lineb in enumerate(f):
                if index == 0:
                    if os.path.isfile(self._write_file):
                        logger.warning(f"Trying to run test when the file exists at {self._write_file}")
                        # Remove the file
                        os.remove(self._write_file)
                line = lineb.decode("utf-8")
                # Change the time column to a datetime object with the start date
                if index > 0:
                    line = line.split(",")
                    # Get the time
                    time_h = float(line[0])
                    # Convert to a datetime object
                    new_time = self._start_datetime + timedelta(hours=time_h)
                    # Replace the time column
                    line[0] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Original time: {line[0]} and new time: {new_time} with start date {self._start_datetime}")
                    line = ",".join(line)

                # Write to
                with open(self._write_file, "a") as write_file:
                    write_file.write(line)
                    logger.info(f"Writing line {index} to {self._write_file}")
                time.sleep(wait)

    def stop(self):
        print("Stopping IndPenSimAdapter")

# import asyncio
# import json
# import logging
# import uuid
# from datetime import datetime, timedelta
#
# from influxobject import InfluxPoint
#
# # from influxobject import InfluxPoint
# # from core.start import get_keydb_client
#
# # Set the logging level
# logging.basicConfig(level=logging.INFO)
#
# # Define a global variable to hold the data
# global_data_lists: list[dict[str, str]] = []
# global_start_time: datetime = datetime.now()
#
# # Function to prepare the data
# def prepare_data(data: dict[str, str]) -> InfluxPoint:
#     try:
#         data = get_global_data()
#         influx_point = InfluxPoint()
#         influx_point.set_measurement('indpensim')
#         influx_point.set_fields(data)
#
#         time_h = float(data['Time (h)'])
#         time = get_global_start_time() + timedelta(hours=time_h)
#         influx_point.set_timestamp(time)
#         influx_point.add_tag('topic', 'leaf-test/indpensim')
#         logging.info("Preparing data: %s", data)
#         return influx_point
#     except Exception as e:
#         logging.error(f"Error in prepare_data: {e}")
#
# async def main() -> None:
#     logging.info("Starting the IndPenSim simulation program")
#     # Monitor the global_data variable if it changes
#     keydb_client = get_keydb_client()
#     size: int = await keydb_client.client.dbsize()
#     logging.info(f"KeyDB client: {keydb_client} of size {size}")
#     while True:
#         raw_data = get_global_data()
#         logging.debug(f"Raw data: {raw_data}")
#         if raw_data:
#             logging.info("Data received")
#             influx_point = prepare_data(raw_data)
#             # Send message to the MQTT broker
#             logging.info("Data: %s", influx_point)
#             # Calculate hash of the data
#             # hash_data = hash(json.dumps(data))
#             hash_data = str(uuid.uuid4())
#             await keydb_client.client.set(hash_data, json.dumps(influx_point.to_json(), indent=4, sort_keys=True))
#             num_keys = await keydb_client.client.dbsize()
#             logging.info(f"Data sent to KeyDB now with a size of {num_keys} entries")
#             set_global_data(None)
#         else:
#             logging.debug("No data received yet")
#         # Sleep for a while
#         await asyncio.sleep(0.1)
#
# # Function to set the global data
# def set_global_data(data: dict[str, str]) -> None:
#     global global_data_lists
#     global_data_lists.append(data)
#     logging.info(f"Contains {len(global_data_lists)} indpensim data sets")
#
# # Function to get the size of the global data
# def get_size_global_data() -> int:
#     global global_data_lists
#     return len(global_data_lists)
#
# # Function to get the global data
# def get_global_data() -> dict[str, str]:
#     global global_data_lists
#     return global_data_lists.pop() if global_data_lists else None
#
# # Function to set the global start time
# def set_global_start_time(start_time: datetime) -> None:
#     global global_start_time
#     global_start_time = start_time
#
# # Function to get the global start time
# def get_global_start_time() -> datetime:
#     global global_start_time
#     return global_start_time
