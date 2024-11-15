import gzip
import logging
import os
import time
import unittest
from datetime import timedelta, datetime
from pathlib import Path
from threading import Thread
from typing import Union, IO, Any, List

import dateparser
import yaml

from leaf.adapters.functional_adapters.table_simulator.adapter import TableSimulatorAdapter
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.output_modules.mqtt import MQTT

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

curr_dir = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(curr_dir, "..", "static_files", "indpensim.yaml"), "r") as file:
    config = yaml.safe_load(file)

watch_file: str = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["watch_file"]
# watch_file: str = os.path.join(curr_dir, "table_simulator_watch_file.csv")
measurement_file: str = curr_dir + "/../" + config["EQUIPMENT_INSTANCES"][0]["equipment"]["simulation"]["filename"]

if not os.path.exists(measurement_file):
    raise FileNotFoundError(f"File {os.path.abspath(measurement_file)} not found")

def _delete_watch_file() -> None:
    watch_file_path = Path(watch_file)
    watch_file_path.unlink(missing_ok=True)

# Returns a gzip file object or a normal file object
def smart_open(filepath: str, mode: str = "rb") -> Union[IO[Any], gzip.GzipFile]:
    """Opens the file with gzip if it ends in .gz, otherwise opens normally."""
    if filepath.endswith(".gz"):
        return gzip.open(filepath, mode)
    else:
        return open(filepath, mode)



class TestSimple(unittest.TestCase):
    def setUp(self) -> None:
        _delete_watch_file()

        broker = config["OUTPUTS"][0]["broker"]
        port = int(config["OUTPUTS"][0]["port"])

        output: MQTT = MQTT(broker, port)
        instance_data: dict[str, str] = {
            "instance_id": "test_IndPenSimAdapter",
            "institute": "test_ins",
        }
        self._time_column = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["time_column"]
        self._start_date = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["start_date"]
        self._interval = config["EQUIPMENT_INSTANCES"][0]["equipment"]["simulation"]["interval"]
        self._sep = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["separator"]
        self._adapter: TableSimulatorAdapter = TableSimulatorAdapter(instance_data=instance_data, output=output, start_date=self._start_date, time_column=self._time_column, write_file=watch_file, sep=",")
        self._mthread = Thread(target=self._adapter.start)
        self._mthread.start()

    def tearDown(self) -> None:
        # Stop the adapter and join the thread
        if self._adapter:
            self._adapter.stop()
        self._mthread.join()
        # Delete the watch file
        _delete_watch_file()

    def test_simple_table_simulator2(self) -> None:
        with smart_open(measurement_file) as f:
            for index, lineb in enumerate(f):
                logger.debug(f"Line {index}")
                try:
                    line_split: List[str] = (
                        lineb.decode("utf-8").strip().split(self._sep)
                    )
                    if index == 0:
                        # Rename the time column to timestamp
                        line_split[line_split.index(self._time_column)] = "timestamp"
                        header = line_split

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
                            if self._start_date != None:
                                # Convert to a datetime object
                                new_time = self._start_date + timedelta(hours=time_h)
                                # Replace the time column
                                line_split[time_index] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                                logger.debug(f"Time value: {time_value} and new time: {line_split[time_index]}")
                            else:
                                logger.warning(f"Start date not defined, using {line_split[time_index]} as the time")
                        except ValueError:
                            logger.debug(f"Time value: {time_value} was not a digit")
                            # Check if the time value is a datetime object
                            time_obj: datetime | None = dateparser.parse(time_value)
                            if time_obj:
                                # Replace the time column
                                line_split[time_index] = time_obj.strftime("%Y-%m-%d %H:%M:%S")
                                logger.debug(f"Time value: {time_value} and time object: {time_obj} and new time: {line_split[time_index]}")
                            else:
                                logger.error(f"Time value: {time_value} could not be converted to a datetime object")
                    # Join the line
                    lineb = f"{self._sep}".join(line_split).encode("utf-8") + b"\n"
                    # Write to
                    with open(watch_file, "ab") as write_file:
                        write_file.write(lineb)
                        logger.info(f"Writing line {index} to {watch_file}")
                except Exception as e:
                    logger.error(f"Error in simulate: {e}")
                time.sleep(self._interval)