import math
import os

import dateparser
import pandas as pd
from influxobject import InfluxPoint
from typing import Optional

from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import InterpreterError

# Note the json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "device.json")

class TableSimulatorInterpreter(AbstractInterpreter):
    def __init__(self, time_column: str, start_date: str, sep: str, 
                 error_holder: Optional[ErrorHolder] = None) -> None:
        super().__init__(error_holder=error_holder)
        self._time_column = time_column
        self._start_date = start_date
        self._sep = sep

    
    def measurement(
        self, data: list[str]) -> InfluxPoint:
        matrix = [
            x[0].split(self._sep) for x in data if isinstance(x, list) and len(x) > 0
        ]
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
                    pass
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
            timestamp = str(last_row_dict["timestamp"])
            time_obj = dateparser.parse(timestamp)
            influx_point.set_timestamp(time_obj)
            influx_point.add_tag("project", "table_simulator")
            # Send message to the MQTT broker
            return influx_point
        except Exception as e:
            excp = InterpreterError(f"Failed to create InfluxPoint: {e}")
            self._handle_exception(excp)

    def metadata(self, data: list[str]) -> dict[str, str]:
        return {"metadata": "Some content", "experiment_id": "undefined"}

    def simulate(self) -> None:
        print("Doing something D?")


# interpreter = TableSimulatorInterpreter()
