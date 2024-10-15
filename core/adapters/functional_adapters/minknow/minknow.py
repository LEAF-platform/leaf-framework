import gzip
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, date
from pprint import pprint
from threading import Thread
from typing import Dict, Union, Any, Optional, List
import dateparser
import grpc
import pandas as pd
from google.protobuf.json_format import MessageToJson
from influxobject import InfluxPoint
from minknow_api.data_pb2_grpc import DataServiceStub
from minknow_api.manager import Manager

from core.adapters.equipment_adapter import AbstractInterpreter, EquipmentAdapter
from core.metadata_manager.metadata import MetadataManager
from core.modules.input_modules.csv_watcher import CSVWatcher
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.phase_modules.initialisation import InitialisationPhase
from core.modules.phase_modules.measurement import MeasurementPhase
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.process_modules.discrete_module import DiscreteProcess

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'table_simulator.json')

# SEPARATOR: str = ","

class MinKNOWInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        logger.info("Initializing MinKNOWInterpreter")
    def measurement(self, data: list[str], measurements: Any) -> Dict[str, Union[str, Dict[str, str], Dict[str, Union[int, float, str]], str]]:
        logger.info(f"TableSimulatoInterpreter data {str(data)[:50]}...")
        # Load measurement into a pd
        # List of lists to DataFrame where the first row is the header
        global SEPARATOR
        matrix = [x[0].split(SEPARATOR) for x in data if isinstance(x, list) and len(x) > 0]
        logger.debug(f"Matrix: {len(matrix)}")
        # TODO Load only the last row
        df = pd.DataFrame([matrix[0], matrix[-1]])
        if df.iloc[0].equals(df.iloc[-1]): return {}
        # Check if there are enough rows
        if df.shape[0] < 2: return {}
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
            elif last_row_dict[key] in ["", 'NaN', None] or (isinstance(last_row_dict[key], float) and math.isnan(last_row_dict[key])):
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
        influx_point.set_measurement('table_simulator')
        influx_point.set_fields(last_row_dict)
        try:
            # time_obj = datetime.strptime(last_row_dict["timestamp"], '%Y-%m-%d %H:%M:%S')
            time_obj = dateparser.parse(last_row_dict["timestamp"])
            logger.debug(f"Time object: {time_obj}")
            influx_point.set_timestamp(time_obj)
            influx_point.add_tag('project', 'table_simulator')
            # Send message to the MQTT broker
            return influx_point.to_json()
        except Exception as e:
            raise BaseException(f"Error in TableSimulatoInterpreter: {e}")
    def metadata(self,data: str) -> dict[str, str]:
        logger.debug(f"Metadata {str(data)[:50]}")
        return {"metadata": "Some content"}
    def simulate(self) -> None:
        logger.error("Simulating TableSimulatorInterpreter")
        print("Doing something D?")


interpreter = MinKNOWInterpreter()


def get_data_from_minknow(position: Any) -> None:
    print("#" * 50)
    # print(position) # gives MS00000 (running)
    connection = position.connect()
    influx_object = InfluxPoint()
    influx_object.set_timestamp(datetime.now())
    influx_object.set_measurement("minknow")
    influx_object.add_tag("position", position)
    # runId = connection.acquisition.get_acquisition_info().run_id???
    # state =
    # writerSummary
    # yieldSummary

    ########################################
    # # Get fan speed
    ########################################
    try:
        fan_speed = connection.minion_device.get_fan_speed()
        # logger.info(f"Fan speed: {fan_speed}")
    except grpc._channel._InactiveRpcError as e:
        pass
        # logger.error(f"gRPC Error: {e.details()}")
        # logger.error(f"Status Code: {e.code()}")

    ########################################
    # Get device settings
    ########################################
    try:
        get_settings = connection.minion_device.get_settings()
        settings_json = MessageToJson(get_settings)
        if not os.path.exists("data"):
            os.makedirs("data")
        with open("data/settings.json", "w") as f:
            f.write(settings_json)
    except grpc._channel._InactiveRpcError as e:
        pass
        # logging.error(f"gRPC Error: {e.details()}")
        # logging.error(f"Status Code: {e.code()}")

    ########################################
    # Testing
    ########################################
    x = connection.device.get_flow_cell_info()
    print(f"Flow Cell Info: {x}")
    # Process all get functions from connection.device
    for i in dir(connection.device):
        print(f"Function: {i}")
        if i.startswith("get"):
            try:
                with open(f"data/device_{i}.json", "w") as f:
                    f.write(MessageToJson(getattr(connection.device, i)()))
                # print(f"Function: {i} -> {getattr(connection.device, i)()}")
            except Exception as e:
                print(f"Function: {i} -> {e}")
    for i in dir(connection.data):
        print(f"Connection: {i}")
        if i.startswith("get"):
            try:
                with open(f"data/connection_{i}.json", "w") as f:
                    f.write(MessageToJson(getattr(connection.data, i)()))
                # print(f"Function: {i} -> {getattr(connection, i)()}")
            except Exception as e:
                print(f"Function: {i} -> {e}")
    wroiuhgwreiogu
    # channel_count = connection.device.get_flow_cell_info().channel_count
    # channel_states_dict = {i: None for i in range(1, channel_count + 1)}
    # logger.info(f"Channel count: {channel_count}")
    # logger.info(f"Obtaining channel states")
    # channel_states = connection.data.get_channel_states(
    #     wait_for_processing=True,
    #     first_channel=1,
    #     last_channel=channel_count,
    # )
    # logger.info(f"Channel states obtained?...")
    # try:
    #     for index, state in enumerate(channel_states):
    #         for channel in state.channel_states:
    #             channel_states_dict[
    #                 int(channel.channel)
    #             ] = channel.state_name
    #             print(f"Channel index {index} {channel.channel} is in state {channel.state_name}"," "*10, end="\r")
    # except Exception as e:
    #     logger.error("Chan state error: {}".format(e))
    #
    # print(channel_states_dict)
    # print(f"Flow Cell Info: {x}")
    # run_id = connection.acquisition.get_acquisition_info().run_id
    # x = connection.statistics.stream_disk_space_info(acquisition_run_id=run_id, _timeout=1)
    # for i in x:
    #     print(i)
    # x = DataServiceStub(connection.channel)
    # print(f"X: {x.__dict__}")
    # print(f"Y: {x.get_channel_states()}")
    # Create a request (you may need to configure this depending on the API)
    # request = GetChannelStatesRequest()


    # print(f"CONNECTION {connection.__dict__}")
    # print(f"DATA{connection.data.__dict__}")
    # print(f"STAT{connection.minion_device.__dict__}")
    ########################################
    # Acquisition
    ########################################
    run_id = connection.acquisition.get_acquisition_info().run_id
    logging.info(f"Run ID: {run_id}")
    influx_object.add_tag("run_id", run_id)
    # print(f"Acquisition: {dir(connection.acquisition)}")
    # print(f"run_id: {connection.acquisition.get_acquisition_info().run_id}")
    acquisition_info = connection.acquisition.get_acquisition_info()
    # Obtain current time stamp
    current_time = time.time()
    if not os.path.exists("data/timer"):
        os.makedirs("data/timer")
    with open(f"data/timer/acquisition_info_{position.device_type}_{current_time}.json", "w") as f:
        content = MessageToJson(acquisition_info)
        f.write(json.dumps(json.loads(content), indent=4, sort_keys=True))

        influx_object.add_field("runId", acquisition_info.run_id)
        # Report writer summary
        writer_summary = acquisition_info.writer_summary
        # logger.debug(f"Writer Summary: {writer_summary}")
        bytes_to_write_produced: int = writer_summary.bytes_to_write_produced
        bytes_to_write_completed: int = writer_summary.bytes_to_write_completed
        influx_object.add_field("bytes_to_write_produced", bytes_to_write_produced)
        influx_object.add_field("bytes_to_write_completed", bytes_to_write_completed)
        # Report yield summary
        read_count = acquisition_info.yield_summary.read_count
        selected_raw_samples = acquisition_info.yield_summary.selected_raw_samples
        selected_events = acquisition_info.yield_summary.selected_events
        estimated_selected_bases = acquisition_info.yield_summary.estimated_selected_bases
        # logger.info(f"Read Count: {read_count}")
        influx_object.add_field("read_count", read_count)
        # logger.info(f"Selected Raw Samples: {selected_raw_samples}")
        influx_object.add_field("selected_raw_samples", selected_raw_samples)
        # logger.info(f"Selected Events: {selected_events}")
        influx_object.add_field("selected_events", selected_events)
        # logger.info(f"Estimated Selected Bases: {estimated_selected_bases}")
        influx_object.add_field("estimated_selected_bases", estimated_selected_bases)
    print(influx_object.to_line_protocol())
        # print(f"Acquisition Info: {content}")
        # purpose = acquisition_info.configuration_summary.purpose
        # logging.info(f"Purpose: {purpose}")
    # for line in open("data/acquisition_info.json"):
    # if "reserved_pore" in line:
    # logging.info(line)

    # print(f"Acquisition Info: {acquisition_info}")

    # Update previous_message with the current one for the next iteration
    # previous_message = acquisition_info


    # return
    # Accessing the acquired and processed values from the response
    progress = connection.acquisition.get_progress()
    acquired = progress.raw_per_channel.acquired
    processed = progress.raw_per_channel.processed


class MinKNOWAdapter(EquipmentAdapter):
    def __init__(self, instance_data: Any, output: Any, write_file: Optional[str], token: Optional[str], host: str = "localhost", port: int = 9501) -> None:
        logger.info(f"Initializing TableSimulator with instance data {instance_data} and output {output} and write file {write_file}")
        # Set variables
        self._host = host
        self._port = port
        self._token = token
        self._manager = Manager(
            host=self._host,
            port=self._port,
            developer_api_token=self._token,
        )
        # Create a metadata manager
        metadata_manager: MetadataManager = MetadataManager()
        metadata_manager.set_metadata("a", "b")
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(write_file, metadata_manager)
        measurements: list[str] = ["Aeration rate(Fg:L/h)"]
        # Create the phases?
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)
        measure_p: MeasurementPhase = MeasurementPhase(output, measurements, metadata_manager)
        details_p: InitialisationPhase = InitialisationPhase(output, metadata_manager)
        self.instance_id: str = instance_data['instance_id']
        self.institute: str = instance_data['institute']
        # global SEPARATOR
        # SEPARATOR = sep
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
        super().__init__(instance_data=instance_data, watcher=watcher, process_adapters=mock_process, interpreter=interpreter, metadata_manager=metadata_manager) # type: ignore
        #instance_data,watcher,mock_process,
        #                 interpreter,metadata_manager=metadata_manager)
        self._write_file = write_file
        # if start_date is not None:
        #     self._start_datetime = datetime.combine(start_date, datetime.min.time())
        self._metadata_manager.add_equipment_data(metadata_fn)


    def simulate(self,filepath:str, delay: int=0, wait: int=0) -> None:
        logger.info("Starting simulation")
        while True:
            logger.info(f"Doing something with {self._host} and {self._port} and {self._token}")
            # Do an API call to a MinKNOW server
            flow_cell_positions = list(self._manager.flow_cell_positions())
            # logger.debug(f"Flow cell positions: {flow_cell_positions}")
            for flow_cell_position in flow_cell_positions:
                # logger.debug(f"Flow cell position: {flow_cell_position}")
                get_data_from_minknow(flow_cell_position)
            time.sleep(1)
        # Finish the simulation
        logger.info("Finished simulation")
        self.stop()


    def stop(self) -> None:
        print("Stopping TableSimulatorAdapter")