import os
import shutil
import sys
import time
import unittest
from pathlib import Path
from threading import Thread
from typing import Any
import tempfile
import yaml
import csv
import uuid

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.adapters.functional_adapters.opentrons.adapter import OpentronsAdapter
from leaf.adapters.functional_adapters.opentrons.interpreter import OpentronsInterpreter
from leaf.modules.output_modules.mqtt import MQTT
from tests.mock_mqtt_client import MockBioreactorClient
from leaf.measurement_terms.manager import measurement_manager

curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir, "..", "test_config.yaml"), "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except KeyError:
    un = None
    pw = None

test_file_dir = os.path.join(curr_dir, "..", "static_files")
initial_file = os.path.join(test_file_dir, "Opentrons_metadata.csv")
measurement_file = os.path.join(test_file_dir, "Opentrons_measurement.csv")
all_data_file = os.path.join(test_file_dir, "Opentrons_full.csv")


def _create_file(watch_file: Path) -> None:
    _delete_watch_file(watch_file)
    shutil.copyfile(initial_file, watch_file)
    time.sleep(1)


def _modify_file(watch_file: Path) -> None:
    with open(measurement_file, "r") as src:
        content = src.read()
    with open(watch_file, "a") as dest:
        dest.write(content)
    time.sleep(1)


def _delete_watch_file(watch_file: Path) -> None:
    watch_file.unlink(missing_ok=True)


class TestOpentronsInterpreter(unittest.TestCase):
    def setUp(self) -> None:
        self._interpreter = OpentronsInterpreter()

    def _metadata_run(self) -> dict[str, Any]:
        with open(initial_file, "r", encoding="latin-1") as file:
            data = list(csv.reader(file, delimiter=";"))
        return self._interpreter.metadata(data)

    def test_metadata(self) -> None:
        result = self._metadata_run()
        self.assertIn("experiment_id", result)
        self.assertIn("sensors", result)

    def test_measurement(self) -> None:
        result = self._metadata_run()
        names = list(result["sensors"].keys())
        measurement_terms = measurement_manager.get_measurements()
        with open(measurement_file, "r", encoding="latin-1") as file:
            data = list(csv.reader(file, delimiter=";"))
        result = self._interpreter.measurement(data)
        for measurement, measurements in result["fields"].items():
            self.assertIn(measurement, measurement_terms)
            for data in measurements:
                self.assertIn(data["name"], names)


class TestOpentrons(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

        unique_id = str(uuid.uuid4())
        self.watch_file = Path(self.temp_dir.name) / f"TestBiolector_{unique_id}.txt"
        self.instance_data = {
            "instance_id": unique_id,
            "institute": f"TestBiolector_{unique_id}_ins",
        }

        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)
        self.output = MQTT(broker, port, username=un, password=pw)
        self._adapter = OpentronsAdapter(
            self.instance_data, self.output, str(self.watch_file)
        )

        self.details_topic = self._adapter._metadata_manager.details()
        self.start_topic = self._adapter._metadata_manager.experiment.start()
        self.stop_topic = self._adapter._metadata_manager.experiment.stop()
        self.running_topic = self._adapter._metadata_manager.running()

        self._flush_topics()
        time.sleep(1)
        wildcard_measure = self._adapter._metadata_manager.experiment.measurement()
        self.mock_client.subscribe(self.start_topic)
        self.mock_client.subscribe(self.stop_topic)
        self.mock_client.subscribe(self.running_topic)
        self.mock_client.subscribe(self.details_topic)
        self.mock_client.subscribe(wildcard_measure)
        time.sleep(1)

    def tearDown(self) -> None:
        self._adapter.stop()
        self._flush_topics()
        self.mock_client.reset_messages()
        self.temp_dir.cleanup()

    def _get_measurements_run(self) -> dict[str, Any]:
        with open(initial_file, "r", encoding="latin-1") as file:
            data = list(csv.reader(file, delimiter=";"))
        self._adapter._interpreter.metadata(data)
        with open(measurement_file, "r", encoding="latin-1") as file:
            data = list(csv.reader(file, delimiter=";"))
        return self._adapter._interpreter.measurement(data)

    def _flush_topics(self) -> None:
        self.mock_client.flush(self.details_topic)
        self.mock_client.flush(self.start_topic)
        self.mock_client.flush(self.stop_topic)
        self.mock_client.flush(self.running_topic)

    def test_details(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(1)
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



if __name__ == "__main__":
    unittest.main()
