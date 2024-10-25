import csv
import gzip
import logging
import os
import time
import unittest
from datetime import timedelta, datetime
from gzip import GzipFile
from pathlib import Path
from threading import Thread
from typing import List, Union, IO, Any
from uuid import uuid4

import dateparser
import yaml

from core.adapters.functional_adapters.table_simulator.adapter import (
    TableSimulatorAdapter,
)
from core.adapters.functional_adapters.table_simulator.adapter import (
    TableSimulatorInterpreter,
)
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.output_modules.mqtt import MQTT
from .. import mock_mqtt_client

curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir, "..","static_files", "indpensim.yaml"), "r") as file:
    config = yaml.safe_load(file)

SEPARATOR: str = ","

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except KeyError:
    un = None
    pw = None

# time_column = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["time_column"]
# start_date = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["start_date"]
# interval = config["EQUIPMENT_INSTANCES"][0]["equipment"]["simulation"]["interval"]

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

watch_file: str = os.path.join(curr_dir, "table_simulator_watch_file.csv")
test_file_dir: str = os.path.join(curr_dir, "..", "static_files")
# measurement_file: str = os.path.join(test_file_dir, "IndPenSim_V3_Batch_1_top10.csv")
measurement_file: str = curr_dir + "/../" + config["EQUIPMENT_INSTANCES"][0]["equipment"]["simulation"]["filename"]
if not os.path.isfile(measurement_file):
    raise FileNotFoundError(f"File {os.path.abspath(measurement_file)} not found")

# Returns a gzip file object or a normal file object
def smart_open(filepath: str, mode: str = "rb") -> Union[IO[Any], GzipFile]:
    """Opens the file with gzip if it ends in .gz, otherwise opens normally."""
    if filepath.endswith(".gz"):
        return gzip.open(filepath, mode)
    else:
        return open(filepath, mode)


# def _create_file() -> None:
#     logger.debug(f"Creating watch file {watch_file}")
#     if os.path.isfile(watch_file):
#         os.remove(watch_file)
#     shutil.copyfile(measurement_file, watch_file)
#     time.sleep(2)


# def _modify_file() -> None:
#     with open(measurement_file, "r") as src:
#         content = src.read()
#     with open(watch_file, "a") as dest:
#         dest.write(content)
#     time.sleep(2)


def _delete_watch_file() -> None:
    watch_file_path = Path(watch_file)
    watch_file_path.unlink(missing_ok=True)


class TestTableSimulatorInterpreter(unittest.TestCase):
    def setUp(self) -> None:
        time_column = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["time_column"]
        start_date = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["start_date"]
        self._interpreter = TableSimulatorInterpreter(time_column, start_date, SEPARATOR)
        self.output: MQTT = MQTT(broker, port, username=un, password=pw)
        self.instance_data: dict[str, str] = {
            "instance_id": "test_IndPenSimAdapter",
            "institute": "test_ins",
        }

        self._adapter: TableSimulatorAdapter = TableSimulatorAdapter(self.instance_data, self.output, watch_file, time_column)


    def _metadata_run(self) -> dict[str, str]:
        # TODO think of a general implementation for the metadata, should this not be in the config?....
        return self._interpreter.metadata(list(""))

    def test_metadata(self) -> None:
        result = self._metadata_run()
        self.assertIn("experiment_id", result)

    def test_measurement(self) -> None:
        # Setting variables from the config
        self.instance_id = config["EQUIPMENT_INSTANCES"][0]["equipment"]["data"]["instance_id"]
        self.institute = config["EQUIPMENT_INSTANCES"][0]["equipment"]["data"]["institute"]
        self._watch_file = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["watch_file"]
        self.time_column = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["time_column"]
        self._start_datetime = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["start_date"]
        self._filename = config["EQUIPMENT_INSTANCES"][0]["equipment"]["simulation"]["filename"]
        self._interval = config["EQUIPMENT_INSTANCES"][0]["equipment"]["simulation"]["interval"]

        logger.info(
            f"Simulating nothing yet for {self.instance_id} at {self.institute} with input file {self._filename} and interval of {self._interval} seconds"
        )

        with smart_open(measurement_file) as f:
            for index, lineb in enumerate(f):
                logger.debug(f"Line {index}")
                try:
                    line_split: List[str] = (
                        lineb.decode("utf-8").strip().split(SEPARATOR)
                    )
                    if index == 0:
                        # Rename the time column to timestamp
                        line_split[line_split.index(self.time_column)] = "timestamp"
                        header = line_split
                        # Checks if file exists and remove if needed
                        if os.path.isfile(self._watch_file):
                            logger.warning(
                                f"Trying to run test when the file exists at {self._watch_file}"
                            )
                            # Remove the file
                            os.remove(self._watch_file)

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
                            time_obj: datetime | None = dateparser.parse(time_value)
                            if time_obj:
                                # Replace the time column
                                line_split[time_index] = time_obj.strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                logger.debug(
                                    f"Time value: {time_value} and time object: {time_obj} and new time: {line_split[time_index]}"
                                )
                            else:
                                logger.error(
                                    f"Time value: {time_value} could not be converted to a datetime object"
                                )
                    # Join the line
                    lineb = f"{SEPARATOR}".join(line_split).encode("utf-8") + b"\n"
                    # Write to
                    with open(watch_file, "ab") as write_file:
                        write_file.write(lineb)
                        logger.info(f"Writing line {index} to {watch_file}")
                except Exception as e:
                    logger.error(f"Error in simulate: {e}")
                time.sleep(self._interval)
        # Finish the simulation
        logger.info("Finished simulation")

class TestTableSimulatorAdapter(unittest.TestCase):
    def setUp(self) -> None:
        time_column = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"]["time_column"]

        logger.debug("Setting up")
        _delete_watch_file()

        self.mock_client: MQTT = mock_mqtt_client.MockBioreactorClient(broker, port, username=un, password=pw)
        logging.debug(f"Broker: {broker} Port: {port} Username: {un}")
        self.output: MQTT = MQTT(broker, port, username=un, password=pw)
        self.instance_data: dict[str, str] = {
            "instance_id": "test_IndPenSimAdapter",
            "institute": "test_ins",
        }

        self._adapter: TableSimulatorAdapter = TableSimulatorAdapter(
            self.instance_data, self.output, watch_file, time_column
        )

        self.details_topic = self._adapter._metadata_manager.details()
        self.start_topic = self._adapter._metadata_manager.experiment.start()
        self.stop_topic = self._adapter._metadata_manager.experiment.stop()
        self.running_topic = self._adapter._metadata_manager.running()

        logger.debug(f"Details topic: {self.details_topic}")
        self._flush_topics()
        time.sleep(2)
        logger.debug(f"Subscribing to {self.start_topic}")
        wildcard_measure = self._adapter._metadata_manager.experiment.measurement()
        self.mock_client.subscribe(self.start_topic)
        self.mock_client.subscribe(self.stop_topic)
        self.mock_client.subscribe(self.running_topic)
        self.mock_client.subscribe(self.details_topic)
        self.mock_client.subscribe(wildcard_measure)
        time.sleep(2)

    def tearDown(self) -> None:
        logger.debug("Tearing down")
        self._adapter.stop()
        self._flush_topics()
        self.mock_client.reset_messages()
        _delete_watch_file()

    def _get_measurements_run(self) -> None:
        logger.debug("Getting measurements")
        with smart_open(measurement_file) as file:
            data = file.readlines()
            with open(watch_file, "w") as f:
                for line in data:
                    f.write(line)
                    # Set to sleep from the config
                    time.sleep(0.1)

        # self._adapter._interpreter.measurement(data)

    def _flush_topics(self) -> None:
        logger.debug("Flushing topics")
        self.mock_client.flush(self.details_topic)
        self.mock_client.flush(self.start_topic)
        self.mock_client.flush(self.stop_topic)
        self.mock_client.flush(self.running_topic)

    def test_details(self) -> None:
        logger.debug("Testing details")
        self._flush_topics()
        self.mock_client.reset_messages()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        self.assertIn(self.details_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.details_topic]) == 1)
        details_data = self.mock_client.messages[self.details_topic][0]
        for k, v in self.instance_data.items():
            self.assertIn(k, details_data)
            self.assertEqual(v, details_data[k])
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_start(self) -> None:
        logger.debug("Testing start")
        self._flush_topics()
        # Making sure we start from a clean slate
        _delete_watch_file()
        self.mock_client.reset_messages()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        # _create_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(1)

        self.assertIn(self.start_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.start_topic]) == 1)
        self.assertIn("experiment_id", self.mock_client.messages[self.start_topic][0])
        self.assertIn(
            self._adapter._interpreter.id,
            self.mock_client.messages[self.start_topic][0]["experiment_id"],
        )
        # TODO why should timestamp be in there?
        # self.assertIn("timestamp", self.mock_client.messages[self.start_topic][0])

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

        _delete_watch_file()
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_stop(self) -> None:
        logger.debug("Testing stop")
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        # time.sleep(2)
        # _create_file()
        time.sleep(2)
        self.mock_client.reset_messages()
        _delete_watch_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        self.assertIn(self.stop_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.stop_topic]) == 1)
        self.assertIn("timestamp", self.mock_client.messages[self.stop_topic][0])

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "False"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

        self.mock_client.messages = {}
        self.mock_client.unsubscribe(self.start_topic)
        self.mock_client.subscribe(self.start_topic)
        self.assertEqual(self.mock_client.messages, {})

        self._flush_topics()
        self.mock_client.reset_messages()

    def test_running(self) -> None:
        logger.debug("Testing running")
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        # time.sleep(2)
        # _create_file()
        time.sleep(2)
        _delete_watch_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

    def test_update(self) -> None:
        logger.debug("Testing update")
        self._flush_topics()
        self.mock_client.reset_messages()
        exp_tp = self._adapter._metadata_manager.experiment.measurement()
        self.mock_client.subscribe(exp_tp)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        # Loop through the measurements and write them to the file
        self._get_measurements_run()
        # time.sleep(2)
        # _create_file()
        # time.sleep(2)
        # _modify_file()
        experiment_id = self._adapter._interpreter.id
        time.sleep(2)
        _delete_watch_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

        self._get_measurements_run()

        for topic in self.mock_client.messages.keys():
            pot_mes = topic.split("/")[-1]
            exp_tp = self._adapter._metadata_manager.experiment.measurement(
                experiment_id=experiment_id, measurement=pot_mes
            )
            if exp_tp in topic:
                data = self.mock_client.messages[exp_tp]
                self.assertTrue(len(data), 1)
                data = data[0]
                self.assertIn("timestamp", data)
                measurement_type = topic.split("/")[-1]
                self.assertIn(measurement_type, actual_mes["measurement"])
                for measurement, measurement_data in data["fields"].items():
                    for md in measurement_data:
                        for am in actual_mes["fields"][measurement]:
                            if am == md:
                                break
                        else:
                            self.fail()
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_logic(self) -> None:
        logger.debug("Testing logic")
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self.assertTrue(len(self.mock_client.messages.keys()) == 1)
        self.assertIn(self.details_topic, self.mock_client.messages)
        time.sleep(2)
        # _create_file()
        self.assertTrue(len(self.mock_client.messages.keys()) == 3)
        self.assertIn(self.start_topic, self.mock_client.messages)
        self.assertIn(self.running_topic, self.mock_client.messages)
        self.assertEqual(len(self.mock_client.messages[self.start_topic]), 1)
        self.assertEqual(
            self.mock_client.messages[self.start_topic][0]["experiment_id"],
            self._adapter._interpreter.id,
        )
        self.assertEqual(len(self.mock_client.messages[self.running_topic]), 1)
        self.assertTrue(self.mock_client.messages[self.running_topic][0] == "True")

        time.sleep(2)
        # _modify_file()
        self.assertTrue(len(self.mock_client.messages.keys()) == 4)
        time.sleep(2)

        self.mock_client.reset_messages()
        _delete_watch_file()
        time.sleep(2)
        self.assertTrue(len(self.mock_client.messages.keys()) == 2)
        self.assertEqual(len(self.mock_client.messages[self.running_topic]), 1)
        self.assertTrue(self.mock_client.messages[self.running_topic][0] == "False")
        self.assertEqual(len(self.mock_client.messages[self.stop_topic]), 1)
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

        self._flush_topics()
        self.mock_client.reset_messages()




if __name__ == "__main__":
    unittest.main()
