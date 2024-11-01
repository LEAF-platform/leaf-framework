import json
import logging
import os
import time
from datetime import datetime
from pprint import pprint
from typing import Dict, Union, Any

import grpc
from google.protobuf.json_format import MessageToJson
from influxobject import InfluxPoint

from core.adapters.equipment_adapter import AbstractInterpreter
from core.modules.logger_modules.logger_utils import get_logger
from minknow_api.manager import Manager, FlowCellPosition
import re
logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "table_simulator.json")

from collections.abc import MutableMapping

def flatten(dictionary: dict[str, Any], parent_key='', separator='_') -> dict[str, Any]:
    items = []
    for key, value in dictionary.items():
        # If key has a capital letter, replace it with an underscore and the lowercase version
        key = re.sub(r"([A-Z])", r"_\1", key).lower()
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)

class MinKNOWInterpreter(AbstractInterpreter):
    def __init__(self, host: str, port: int, token: str) -> None:
        super().__init__()
        logger.info("Initializing MinKNOWInterpreter")
        self._host = host
        self._port = port
        self._token = token

    def measurement(self, data: str) -> InfluxPoint:
        manager = self.get_manager()
        # Get the position of the device
        position = manager.flow_cell_positions()
        for pos in position:
            print(f"Position: {pos}")
            # Get data from the device
            influx_object = self.get_data_from_minknow(pos)
            return influx_object

    def metadata(self, data: str) -> dict[str, str]:
        logger.debug(f"Metadata {str(data)[:50]}")
        return {"metadata": "Some content"}

    def simulate(self) -> None:
        pass

    def get_manager(self) -> Manager:
        # Construct a manager using the host + port provided:
        manager = Manager(
        host=self._host,
        port=self._port,
        developer_api_token=self._token,
        )
        return manager

    def get_data_from_minknow(sefl, position: FlowCellPosition) -> InfluxPoint:
        print("#" * 50)
        # print(position) # gives MS00000 (running)
        connection = position.connect()
        influx_object = InfluxPoint()
        influx_object.set_timestamp(datetime.now())
        influx_object.set_measurement("minknow")
        influx_object.add_field("field", 1)

        acquisition_info = connection.acquisition.get_acquisition_info()
        acquisition_info_json = MessageToJson(acquisition_info)
        acquisition_info_dict = json.loads(acquisition_info_json)
        acquisition_info_dict = flatten(acquisition_info_dict)
        # print(f"Run ID: {acquisition_info_dict}")
        for k, v in acquisition_info_dict.items():
            try:
                influx_object.add_field(k, v)
            except TypeError as e:
                logger.error(f"Error: {e}")

        ########################################
        # Get device settings
        ########################################
        get_settings = connection.minion_device.get_settings()
        settings_json = MessageToJson(get_settings)
        settings_dict = json.loads(settings_json)
        settings_dict = flatten(settings_dict)
        for k, v in settings_dict.items():
            influx_object.add_field(k, v)
        ########################################
        # Get flow cell info
        flow_cell_info = connection.device.get_flow_cell_info()
        flow_cell_info_json = MessageToJson(flow_cell_info)
        flow_cell_info_dict = json.loads(flow_cell_info_json)
        flow_cell_info_dict = flatten(flow_cell_info_dict)
        for k, v in flow_cell_info_dict.items():
            influx_object.add_tag(k, v)

        # Get experiment yield info
        experiment_yield_info = connection.data.get_experiment_yield_info()
        experiment_yield_info_json = MessageToJson(experiment_yield_info)
        # Flatten the json
        experiment_yield_info_dict = json.loads(experiment_yield_info_json)
        flattened_experiment_yield_info = flatten(experiment_yield_info_dict)
        for k, v in flattened_experiment_yield_info.items():
            influx_object.add_field(k, v)

        ########################################
        # Tags / fields are automatically added to the influx object so lets prune a bit where needed
        ########################################

        remove_fields = set()
        for k, v in influx_object.fields.items():
            if type(v) == list:
                remove_fields.add(k)
            if type(v) == str and (v.endswith('_BIAS_VOLTAGE') or k.startswith('channel_config_')):
                remove_fields.add(k)
        for k in remove_fields:
            influx_object.fields.pop(k)
        pprint(influx_object.to_json(), indent=4)

        return influx_object