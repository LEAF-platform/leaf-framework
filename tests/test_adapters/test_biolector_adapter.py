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

from leaf.measurement_terms.manager import measurement_manager

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.adapters.functional_adapters.biolector1.adapter import Biolector1Adapter
from leaf.adapters.functional_adapters.biolector1.interpreter import (
    Biolector1Interpreter,
)
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
initial_file = os.path.join(test_file_dir, "biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir, "biolector1_measurement.csv")
all_data_file = os.path.join(test_file_dir, "biolector1_full.csv")


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


class TestBiolector1Interpreter(unittest.TestCase):
    def setUp(self) -> None:
        self._interpreter = Biolector1Interpreter()

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


class TestBiolector1(unittest.TestCase):

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
        self._adapter = Biolector1Adapter(
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

    def test_start(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(1)
        _create_file(self.watch_file)
        time.sleep(1)
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
        self.assertIn("timestamp", self.mock_client.messages[self.start_topic][0])

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

        _delete_watch_file(self.watch_file)
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_stop(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(1)
        _create_file(self.watch_file)
        time.sleep(1)
        self.mock_client.reset_messages()
        _delete_watch_file(self.watch_file)
        time.sleep(1)
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
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(1)
        _create_file(self.watch_file)
        time.sleep(1)
        _delete_watch_file(self.watch_file)
        time.sleep(1)
        self._adapter.stop()
        mthread.join()

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

    def test_update(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()
        exp_tp = self._adapter._metadata_manager.experiment.measurement()
        self.mock_client.subscribe(exp_tp)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(1)
        _create_file(self.watch_file)
        time.sleep(1)
        _modify_file(self.watch_file)
        experiment_id = self._adapter._interpreter.id
        time.sleep(1)
        _delete_watch_file(self.watch_file)
        time.sleep(1)
        self._adapter.stop()
        mthread.join()
        time.sleep(1)

        actual_mes = self._get_measurements_run()
        expected_measurements = ["Biomass", "GFP", "mCherrry/RFPII", "pH-hc", "pO2-hc"]
        seens = []
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
                            self.assertIn("value", am)
                            if am == md:
                                break
                        else:
                            self.fail()
                        name = md["name"]
                        self.assertIn(name, expected_measurements)
                        if name not in seens:
                            seens.append(name)
        self.assertCountEqual(seens, expected_measurements)
        self._flush_topics()
        self.mock_client.reset_messages()


if __name__ == "__main__":
    unittest.main()
