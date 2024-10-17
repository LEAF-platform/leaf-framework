import csv
import logging
import os
import shutil
import time
import unittest
from threading import Thread
from typing import Any

import yaml

from core.adapters.functional_adapters.table_simulator.table_simulator import (
    TableSimulatorAdapter,
)
from core.adapters.functional_adapters.table_simulator.table_simulator import (
    TableSimulatorInterpreter,
)
from core.measurement_terms.manager import measurement_manager
from core.modules.output_modules.mqtt import MQTT
from mock_mqtt_client import MockBioreactorClient

curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir, "data", "indpensim.yaml"), "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except:
    un = None
    pw = None
time_column = config["EQUIPMENT_INSTANCES"][0]["equipment"]["requirements"][
    "time_column"
]

watch_file: str = os.path.join("tmp.txt")
test_file_dir: str = os.path.join(curr_dir, "data")
measurement_file: str = os.path.join(test_file_dir, "IndPenSim_V3_Batch_1_top10.csv")


def _create_file() -> None:
    if os.path.isfile(watch_file):
        os.remove(watch_file)
    shutil.copyfile(measurement_file, watch_file)
    time.sleep(2)


def _modify_file() -> None:
    with open(measurement_file, "r") as src:
        content = src.read()
    with open(watch_file, "a") as dest:
        dest.write(content)
    time.sleep(2)


def _delete_file() -> None:
    if os.path.isfile(watch_file):
        os.remove(watch_file)


class TestTableSimulatorInterpreter(unittest.TestCase):
    def setUp(self) -> None:
        self._interpreter = TableSimulatorInterpreter()

    def _metadata_run(self) -> dict[str, str]:
        with open(measurement_file, "r", encoding="latin-1") as file:
            data = list(csv.reader(file, delimiter=";"))
        return self._interpreter.metadata(data)

    def test_metadata(self) -> None:
        result = self._metadata_run()
        self.assertIn("experiment_id", result)

    def test_measurement(self) -> None:
        # TODO - Fix this test, it failes as the simulator fixes the time header...
        #  but this function is not called in this test case
        result = self._metadata_run()
        measurement_terms = measurement_manager.get_measurements()
        with open(measurement_file, "r", encoding="latin-1") as file:
            data = list(csv.reader(file, delimiter=";"))
        result = self._interpreter.measurement(data, None)
        print(result)

    def test_simulate(self) -> None:
        pass


class TestTableSimulatorAdapter(unittest.TestCase):

    def setUp(self):
        if os.path.isfile(watch_file):
            os.remove(watch_file)

        self.mock_client: MQTT = MockBioreactorClient(broker, port, username=un, password=pw)
        logging.debug(f"Broker: {broker} Port: {port} Username: {un}")
        self.output: MQTT = MQTT(broker, port, username=un, password=pw)
        self.instance_data: dict = {
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

        self._flush_topics()
        time.sleep(2)
        wildcard_measure = self._adapter._metadata_manager.experiment.measurement()
        self.mock_client.subscribe(self.start_topic)
        self.mock_client.subscribe(self.stop_topic)
        self.mock_client.subscribe(self.running_topic)
        self.mock_client.subscribe(self.details_topic)
        self.mock_client.subscribe(wildcard_measure)
        time.sleep(2)

    def tearDown(self) -> None:
        self._adapter.stop()
        self._flush_topics()
        self.mock_client.reset_messages()

    def _get_measurements_run(self):
        with open(measurement_file, "r", encoding="latin-1") as file:
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
        self._flush_topics()
        self.mock_client.reset_messages()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
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
        self.assertIn("timestamp", self.mock_client.messages[self.start_topic][0])

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

        os.remove(watch_file)
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_stop(self) -> None:
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        time.sleep(2)
        self.mock_client.reset_messages()
        _delete_file()
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
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        time.sleep(2)
        _delete_file()
        time.sleep(2)
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
        time.sleep(2)
        _create_file()
        time.sleep(2)
        _modify_file()
        experiment_id = self._adapter._interpreter.id
        time.sleep(2)
        _delete_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

        actual_mes = self._get_measurements_run()

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
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self.assertTrue(len(self.mock_client.messages.keys()) == 1)
        self.assertIn(self.details_topic, self.mock_client.messages)
        time.sleep(2)
        _create_file()
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
        _modify_file()
        self.assertTrue(len(self.mock_client.messages.keys()) == 4)
        time.sleep(2)

        self.mock_client.reset_messages()
        _delete_file()
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
