import os
import shutil
import sys
import time
import unittest
from threading import Thread
import yaml
import tempfile
import uuid

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from core.adapters.functional_adapters.biolector1.biolector1 import Biolector1Adapter
from core.modules.output_modules.mqtt import MQTT
from ..mock_mqtt_client import MockBioreactorClient

import logging

logging.basicConfig(level=logging.DEBUG)


curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(curr_dir + "/../test_config.yaml", "r") as file:
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


def _create_file(adapter):
    watch_file = adapter._write_file
    if os.path.isfile(watch_file):
        os.remove(watch_file)
    shutil.copyfile(initial_file, watch_file)
    time.sleep(1)


def _modify_file(adapter):
    watch_file = adapter._write_file
    with open(measurement_file, "r") as src:
        content = src.read()
    with open(watch_file, "a") as dest:
        dest.write(content)
    time.sleep(1)


def _delete_file(adapter):
    watch_file = adapter._write_file
    if os.path.isfile(watch_file):
        os.remove(watch_file)


class TestAdapterArray(unittest.TestCase):
    def setUp(self):

        self.temp_dir = tempfile.TemporaryDirectory()

        unique_id1 = str(uuid.uuid4())
        unique_id2 = str(uuid.uuid4())
        self.watch_file1 = os.path.join(
            self.temp_dir.name, f"TestAdapterArray_{unique_id1}.txt"
        )
        self.watch_file2 = os.path.join(
            self.temp_dir.name, f"TestAdapterArray_{unique_id2}.txt"
        )

        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)
        logging.debug(f"Broker: {broker} Port: {port} Username: {un}")
        self.output = MQTT(broker, port, username=un, password=pw)

        instance_data1 = {
            "instance_id": f"TestAdapterArray_{unique_id1}",
            "institute": f"Institute_{unique_id1}",
        }
        self._biolector = Biolector1Adapter(
            instance_data1, self.output, self.watch_file1
        )

        instance_data2 = {
            "instance_id": f"TestAdapterArray_{unique_id2}",
            "institute": f"Institute_{unique_id2}",
        }
        self._biolector2 = Biolector1Adapter(
            instance_data2, self.output, self.watch_file2
        )

        self.adapter_array = [self._biolector, self._biolector2]

        for adapter in self.adapter_array:
            details_topic = adapter._metadata_manager.details()
            start_topic = adapter._metadata_manager.experiment.start()
            stop_topic = adapter._metadata_manager.experiment.stop()
            running_topic = adapter._metadata_manager.running()

            self.mock_client.flush(details_topic)
            self.mock_client.flush(start_topic)
            self.mock_client.flush(stop_topic)
            self.mock_client.flush(running_topic)
            time.sleep(1)
            wildcard_measure = adapter._metadata_manager.experiment.measurement()
            self.mock_client.subscribe(start_topic)
            self.mock_client.subscribe(stop_topic)
            self.mock_client.subscribe(running_topic)
            self.mock_client.subscribe(details_topic)
            self.mock_client.subscribe(wildcard_measure)
            time.sleep(1)

    def tearDown(self):
        for adapter in self.adapter_array:
            self._flush_topics(adapter)

        self.temp_dir.cleanup()

    def _run_adapters(self):
        adapter_threads = []
        for adapter in self.adapter_array:
            thread = Thread(target=adapter.start)
            thread.daemon = True
            thread.start()
            adapter_threads.append(thread)
        return adapter_threads

    def _stop_adapters(self, adapter_threads):
        for adapter in self.adapter_array:
            adapter.stop()
        for thread in adapter_threads:
            thread.join()

    def test_details(self):
        threads = self._run_adapters()
        time.sleep(1)
        self._stop_adapters(threads)
        for adapter in self.adapter_array:
            details_topic = adapter._metadata_manager.details()
            self.assertIn(details_topic, self.mock_client.messages)
            self.assertTrue(len(self.mock_client.messages[details_topic]) == 1)
            details_data = self.mock_client.messages[details_topic][0]

            self.assertIn("instance_id", details_data)
            self.assertEqual(
                adapter._metadata_manager.get_equipment_data()["instance_id"],
                details_data["instance_id"],
            )

    def test_start(self):
        threads = self._run_adapters()
        time.sleep(1)
        for adapter in self.adapter_array:
            _create_file(adapter)
        time.sleep(1)
        self._stop_adapters(threads)

        for adapter in self.adapter_array:
            start_topic = adapter._metadata_manager.experiment.start()
            self.assertIn(start_topic, self.mock_client.messages)
            self.assertTrue(len(self.mock_client.messages[start_topic]) == 1)
            self.assertIn("experiment_id", self.mock_client.messages[start_topic][0])
            self.assertIn(
                adapter._interpreter.id,
                self.mock_client.messages[start_topic][0]["experiment_id"],
            )
            self.assertIn("timestamp", self.mock_client.messages[start_topic][0])

            running_topic = adapter._metadata_manager.running()
            self.assertIn(running_topic, self.mock_client.messages)
            expected_run = "True"
            self.assertEqual(self.mock_client.messages[running_topic][0], expected_run)

    def test_stop(self):
        threads = self._run_adapters()
        time.sleep(1)
        for adapter in self.adapter_array:
            _create_file(adapter)
            time.sleep(1)
            _delete_file(adapter)
            time.sleep(1)
        time.sleep(1)
        self._stop_adapters(threads)

        for adapter in self.adapter_array:
            stop_topic = adapter._metadata_manager.experiment.stop()
            running_topic = adapter._metadata_manager.running()
            self.assertIn(stop_topic, self.mock_client.messages)
            self.assertTrue(len(self.mock_client.messages[stop_topic]) == 1)
            self.assertIn("timestamp", self.mock_client.messages[stop_topic][0])

            self.assertIn(running_topic, self.mock_client.messages)
            expected_run = "False"
            self.assertEqual(self.mock_client.messages[running_topic][1], expected_run)

    def test_running(self):
        threads = self._run_adapters()
        time.sleep(1)
        for adapter in self.adapter_array:
            _create_file(adapter)
            time.sleep(1)
            _delete_file(adapter)
            time.sleep(1)
        time.sleep(1)
        self._stop_adapters(threads)

        for adapter in self.adapter_array:
            running_topic = adapter._metadata_manager.running()

            self.assertIn(running_topic, self.mock_client.messages)
            expected_run = "True"
            self.assertEqual(self.mock_client.messages[running_topic][0], expected_run)

    def test_logic(self):
        threads = self._run_adapters()
        self.mock_client.reset_messages()
        time.sleep(1)
        details_topics = []
        for adapter in self.adapter_array:
            details_topics.append(adapter._metadata_manager.details())
        for adapter in self.adapter_array:
            self._flush_topics(adapter)

            details_topic = adapter._metadata_manager.details()
            start_topic = adapter._metadata_manager.experiment.start()
            stop_topic = adapter._metadata_manager.experiment.stop()
            running_topic = adapter._metadata_manager.running()

            self.assertTrue(len(self.mock_client.messages.keys()) == 2)
            self.assertIn(details_topic, self.mock_client.messages)
            exp_tp = adapter._metadata_manager.experiment.measurement()
            self.mock_client.subscribe(exp_tp)
            time.sleep(1)
            _create_file(adapter)
            self.assertTrue(len(self.mock_client.messages.keys()) == 4)
            self.assertIn(start_topic, self.mock_client.messages)
            self.assertIn(running_topic, self.mock_client.messages)
            self.assertEqual(len(self.mock_client.messages[start_topic]), 1)
            self.assertEqual(
                self.mock_client.messages[start_topic][0]["experiment_id"],
                adapter._interpreter.id,
            )
            self.assertEqual(len(self.mock_client.messages[running_topic]), 1)
            self.assertTrue(self.mock_client.messages[running_topic][0] == "True")
            time.sleep(1)
            _modify_file(adapter)
            self.assertTrue(len(self.mock_client.messages.keys()) == 5)
            time.sleep(1)
            _delete_file(adapter)
            time.sleep(1)
            self.assertTrue(len(self.mock_client.messages.keys()) == 6)
            self.assertEqual(len(self.mock_client.messages[running_topic]), 2)
            self.assertTrue(self.mock_client.messages[running_topic][1] == "False")
            self.assertEqual(len(self.mock_client.messages[stop_topic]), 1)
            time.sleep(1)
            self._flush_topics(adapter)

            for k in list(self.mock_client.messages.keys()):
                if k not in details_topics:
                    del self.mock_client.messages[k]

        self._stop_adapters(threads)

        self.mock_client.reset_messages()

    def _flush_topics(self, adapter):
        details_topic = adapter._metadata_manager.details()
        start_topic = adapter._metadata_manager.experiment.start()
        stop_topic = adapter._metadata_manager.experiment.stop()
        running_topic = adapter._metadata_manager.running()
        self.mock_client.flush(details_topic)
        self.mock_client.flush(start_topic)
        self.mock_client.flush(stop_topic)
        self.mock_client.flush(running_topic)


if __name__ == "__main__":
    unittest.main()
