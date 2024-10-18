import logging
import math
import os
from typing import Dict, Union

import dateparser
import pandas as pd
from influxobject import InfluxPoint

from core.adapters.equipment_adapter import AbstractInterpreter
from core.modules.logger_modules.logger_utils import get_logger

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
        self, data: list[str]
    ) -> Dict[str, Union[str, Dict[str, str], Dict[str, Union[int, float, str]], str]]:
        logger.info(f"TableSimulatoInterpreter data {str(data)[:50]}...")
        # Load measurement into a pd
        # List of lists to DataFrame where the first row is the header
        global SEPARATOR #:(
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

    def metadata(self, data: list[str]) -> dict[str, str]:
        logger.debug(f"Metadata {str(data)[:50]}")
        return {"metadata": "Some content", "experiment_id": "THIS IS WRONG"}

    def simulate(self) -> None:
        logger.error("Simulating TableSimulatorInterpreter")
        print("Doing something D?")


interpreter = TableSimulatorInterpreter()
