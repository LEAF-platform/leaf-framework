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
from leaf.measurement_handler.terms import measurement_manager

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
initial_file = os.path.join(test_file_dir, "temp_empty_file.keep_log")
measurement_file = os.path.join(test_file_dir, "opentrons_log_data.keep_log")


def _start_experiment(watch_file: Path) -> None:
    _stop_experiment(watch_file)
    shutil.copyfile(initial_file, watch_file)
    time.sleep(1)


def _update_experiment(watch_file: Path) -> None:
    with open(measurement_file, "r") as src:
        content = src.read()
    with open(watch_file, "a") as dest:
        dest.write(content)
    time.sleep(1)


def _stop_experiment(watch_file: Path) -> None:
    watch_file.unlink(missing_ok=True)


class TestOpentronsInterpreter(unittest.TestCase):
    def setUp(self) -> None:
        self._interpreter = OpentronsInterpreter()

    def _metadata_run(self) -> dict[str, Any]:
        data = {}
        return self._interpreter.metadata(data)

    def test_metadata(self) -> None:
        result = self._metadata_run()
        self.assertIn("experiment_id", result)
        self.assertIn("sensors", result)

    def test_measurement(self) -> None:
        result = self._metadata_run()
        with open(measurement_file, "r", encoding="latin-1") as file:
            data = file.readlines()
        result = self._interpreter.measurement(data)
        self.assertGreater(len(result), 0)

        commands = [point.fields.get("command") for point in result]
        self.assertIn("RETURN_TIP", commands)
        self.assertIn("api_version", commands)

        details = [point.fields.get("details") for point in result]
        self.assertTrue(any("OT2CEP" in detail for detail in 
                            details if detail))
        


class TestOpentrons(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()

        unique_id = str(uuid.uuid4())
        self.watch_file = Path(self.temp_dir.name) / f"TestOpentrons_{unique_id}.txt"
        self.instance_data = {
            "instance_id": unique_id,
            "institute": f"TestOpentrons_{unique_id}_ins",
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
        time.sleep(0.1)
        self.mock_client.subscribe(self.stop_topic)
        time.sleep(0.1)
        self.mock_client.subscribe(self.running_topic)
        time.sleep(0.1)
        self.mock_client.subscribe(self.details_topic)
        time.sleep(0.1)
        self.mock_client.subscribe(wildcard_measure)
        time.sleep(1)

    def tearDown(self) -> None:
        self._adapter.stop()
        self._flush_topics()
        self.mock_client.reset_messages()
        self.temp_dir.cleanup()

    def _get_measurements_run(self) -> dict[str, Any]:
        with open(initial_file, "r", encoding="latin-1") as file:
            data = file.readlines()
        self._adapter._interpreter.metadata(data)
        with open(measurement_file, "r", encoding="latin-1") as file:
            data = file.readlines()
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
        _start_experiment(self.watch_file)
        time.sleep(1)
        self._adapter.stop()
        mthread.join()
        time.sleep(1)

        self.assertIn(self.start_topic, self.mock_client.messages,self.mock_client.messages.keys())
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

        _stop_experiment(self.watch_file)
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_stop(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(1)
        _start_experiment(self.watch_file)
        time.sleep(1)
        self.mock_client.reset_messages()
        _stop_experiment(self.watch_file)
        time.sleep(1)
        self._adapter.stop()
        mthread.join()
        self.assertIn(self.stop_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.stop_topic]) == 1)

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
        _start_experiment(self.watch_file)
        time.sleep(1)
        _stop_experiment(self.watch_file)
        time.sleep(1)
        self._adapter.stop()
        mthread.join()

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], 
                         expected_run)

    def test_update(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()
        exp_tp = self._adapter._metadata_manager.experiment.measurement()
        self.mock_client.subscribe(exp_tp)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(0.1)
        _start_experiment(self.watch_file)
        time.sleep(0.1)
        _update_experiment(self.watch_file)
        experiment_id = self._adapter._interpreter.id
        time.sleep(0.1)
        _stop_experiment(self.watch_file)
        time.sleep(0.1)
        self._adapter.stop()
        mthread.join()
        time.sleep(0.1)

        actual_mes = self._get_measurements_run()
        for topic in self.mock_client.messages.keys():
            pot_mes = topic.split("/")[-1]
            exp_tp = self._adapter._metadata_manager.experiment.measurement(
                experiment_id=experiment_id, measurement=pot_mes
            )
            if exp_tp in topic:
                data = self.mock_client.messages[exp_tp]
                for chunk in data:
                    for measurement in chunk:
                        self.assertIn("timestamp", measurement)
                        for m_type, value in measurement["fields"].items():
                            found = False
                            for meas in actual_mes:
                                for a_measurement_type,a_measurement_value in meas.fields.items():
                                    if a_measurement_type == m_type and a_measurement_value == value:
                                        found = True
                                        break
                                if found:
                                    break
                            else:
                                self.fail()
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_simulate(self):
        self._adapter.simulate(measurement_file)
if __name__ == "__main__":
    unittest.main()
