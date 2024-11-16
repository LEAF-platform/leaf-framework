import logging
import os
import re
from datetime import datetime, timezone
from typing import MutableMapping, Any, List,Optional

import dateparser
import requests
from influxobject import InfluxPoint

from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.error_handler.error_holder import ErrorHolder

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "device.json")

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


class MAQInterpreter(AbstractInterpreter):
    # '<institute>/<equipment_id>/<instance_id>/details'
    def __init__(self, token: str,error_holder: Optional[ErrorHolder] = None) -> None:
        super().__init__(error_holder=error_holder)
        logger.info("Initializing MAQInterpreter")
        self._host = "https://www.maq-observations.nl"
        self._token = token
        self.id = "default_maq_id"
        self._processed_time: set[str] = set()
        # No idea...
        self.EXPERIMENT_ID_KEY = "experiment_id"
        self.MEASUREMENT_HEADING_KEY = "measurement"
        self.TIMESTAMP_KEY = "timestamp"
        # Currently only a subset of the streams is processed
        self._streams: set[str] = {"TA_1_1_1", "RH_1_1_1", "SW_IN_1_1_1", "PA_1_1_1", "WS_1_1_1", "WD_1_1_1", "P_1_1_1"}
        self._sensor_last_timestamp: dict[str, datetime] = {}

    def retrieval(self) -> dict[str, Any]:
        logger.debug(f"Retrieval...")
        return {"measurement": "some data?", "start": None, "stop": None}

    def measurement(self, ignore) -> List[InfluxPoint]:
        influx_object = InfluxPoint()
        # Get the data
        headers = {'Accept': 'application/json', 'Authorization': 'ApiKey {}'.format(self._token), 'Content-Type':'text/csv'}

        #GET STREAMS VEENKAMPEN
        END_POINT = '/wp-json/maq/v1/sites/1/stations/1/streams'
        get = requests.get(self._host + END_POINT, headers=headers)
        data = get.json()
        logger.debug(f"Data {data.keys()}")
        lookup: dict[str, Any] = {}
        for stream in data['streams']:
            # logger.debug(f"Stream {stream}")
            stream = flatten(stream)
            lookup[stream['name']] = stream

        # GET DATA VEENKAMPEN
        data_store: dict[str, Any] = {}
        for index, key in enumerate(lookup.keys()):
            if key not in self._streams:
                continue
            logger.debug(f"Processing {key} {index} of {len(lookup.keys())}")
            stream = lookup[key]['id']
            logger.debug(f"Stream {stream}")
            END_POINT = f'/wp-json/maq/v1/streams/{stream}/measures' # ?from=2023-01-01&to=2023-01-02'
            get = requests.get(self._host + END_POINT, headers=headers)
            data = get.json()
            for measure in data['measures']:
                # print(measure)
                if measure['timestamp'] not in data_store:
                    data_store[measure['timestamp']] = {}
                data_store[measure['timestamp']][key] = measure['value']
                # print(data_store)
        print("-----")
        # Reformat the data
        influx_objects = []
        latest_timestamp: datetime = dateparser.parse("1900-01-01").replace(tzinfo=timezone.utc)
        for timestamp in data_store:
            date_time: datetime|None = dateparser.parse(timestamp)
            if date_time is None:
                logger.error(f"Could not parse date {timestamp}")
                continue
            # For each timestamp and each stream an influx object is created
            for stream in data_store[timestamp]:
                if stream not in self._sensor_last_timestamp:
                    self._sensor_last_timestamp[stream] = date_time
                if date_time <= self._sensor_last_timestamp[stream]:
                    logger.debug(f"Skipping {stream} {date_time} < {self._sensor_last_timestamp[stream]}")
                    continue
                if date_time > self._sensor_last_timestamp[stream]:
                    self._sensor_last_timestamp[stream] = date_time
                influx_object = InfluxPoint()
                influx_object.set_measurement("maq_observations")
                # Add the metadata tags
                for k, v in lookup[stream].items():
                    # Replace the degree symbol by an ascii equivalent
                    if isinstance(v, str):
                        v = v.replace("°", "*")
                    influx_object.add_tag(k, v)
                influx_object.set_timestamp(date_time)
                influx_object.add_field("value", data_store[timestamp][stream])
                # Add the influx object to the list
                influx_objects.append(influx_object)
        logger.debug(f"Number of influx objects to be submitted: {len(influx_objects)}")
        return influx_objects

    def simulate(self) -> None:
        pass

    def metadata(self, data: str) -> dict[str, str]:
        logger.debug(f"Metadata {str(data)[:50]}")
        return {"metadata": "Some content"}