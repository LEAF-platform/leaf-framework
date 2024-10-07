import os
import sys
import unittest
import time
import yaml
import shutil
import csv
from threading import Thread

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from core.adapters.functional_adapters.biolector1.biolector1 import Biolector1Adapter
from core.modules.output_modules.mqtt import MQTT
from mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import metadata_manager

with open('../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)
broker = config["OUTPUT"]["broker_address"]
port = int(config["OUTPUT"]["port"])
un = config["OUTPUT"]["username"]
pw = config["OUTPUT"]["password"]
watch_file = os.path.join("tmp.txt")
curr_dir = os.path.dirname(os.path.realpath(__file__))
test_file_dir = os.path.join(curr_dir,"..","static_files")
initial_file = os.path.join(test_file_dir,"biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir,"biolector1_measurement.csv")
all_data_file = os.path.join(test_file_dir,"biolector1_full.csv")
text_watch_file = os.path.join("tmp.txt")

def _create_file():
    if os.path.isfile(watch_file):
        os.remove(watch_file)
    shutil.copyfile(initial_file, watch_file)
    time.sleep(2)

def _modify_file():
    with open(measurement_file, 'r') as src:
        content = src.read()
    with open(watch_file, 'a') as dest:
        dest.write(content)
    time.sleep(2)

def _delete_file():
    if os.path.isfile(watch_file):
        os.remove(watch_file)

class TestBiolector1(unittest.TestCase):
    def setUp(self):
        if os.path.isfile(watch_file):
            os.remove(watch_file)

        self.mock_client = MockBioreactorClient(broker, port, 
                                                username=un,password=pw)
        self.output = MQTT(broker,port,username=un,password=pw)
        self.instance_data = {"instance_id" : "test_biolector123","institute" : "test_ins"}
        self._adapter = Biolector1Adapter(self.instance_data,
                                          self.output,
                                          watch_file)
        self.details_topic = metadata_manager.details()
        self.start_topic = metadata_manager.experiment.start()
        self.stop_topic = metadata_manager.experiment.stop()
        self.running_topic = metadata_manager.running()

        self.mock_client.flush(self.details_topic)
        self.mock_client.flush(self.start_topic)
        self.mock_client.flush(self.stop_topic)
        self.mock_client.flush(self.running_topic)
        time.sleep(2)
        wildcard_measure = metadata_manager.experiment.measurement()
        self.mock_client.subscribe(self.start_topic)
        self.mock_client.subscribe(self.stop_topic)
        self.mock_client.subscribe(self.running_topic)
        self.mock_client.subscribe(self.details_topic)
        self.mock_client.subscribe(wildcard_measure)
        time.sleep(2)

    def test_details(self):
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        self.assertIn(self.details_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.details_topic]) == 1)
        details_data = self.mock_client.messages[self.details_topic][0]
        for k,v in self.instance_data.items():
            self.assertIn(k,details_data)
            self.assertEqual(v,details_data[k])

    def test_start(self):
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        time.sleep(2)
        _delete_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()

        self.assertIn(self.start_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.start_topic]) == 1)
        self.assertIn("experiment_id", self.mock_client.messages[self.start_topic][0])
        self.assertIn(self._adapter._interpreter.id, self.mock_client.messages[self.start_topic][0]["experiment_id"])
        self.assertIn("timestamp", self.mock_client.messages[self.start_topic][0])

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)
    
    def test_stop(self):
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
        self.assertEqual(self.mock_client.messages,{})

    def test_running(self):
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

    def test_update(self):
        exp_tp = metadata_manager.experiment.measurement()
        self.mock_client.subscribe(exp_tp)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        time.sleep(2)
        _modify_file()
        exp_tp = metadata_manager.experiment.measurement(experiment_id=self._adapter._interpreter.id)
        time.sleep(2)
        _delete_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

        self.assertIn(exp_tp, self.mock_client.messages)
        expected_measurements = ["Biomass","GFP","mCherrry/RFPII","pH-hc","pO2-hc"]
        data = self.mock_client.messages[exp_tp]
        for message in data:
            message = message[self._adapter._interpreter.UPDATE_KEY]
            for measurement,wells in message.items():
                self.assertIn(measurement,expected_measurements)
                expected_measurements.pop(
                    expected_measurements.index(measurement))
                for well,wdata in wells.items():
                    self.assertIn("value",wdata)
        self.assertEqual(len(expected_measurements),0,expected_measurements)        

    def test_logic(self):
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self.assertTrue(len(self.mock_client.messages.keys()) == 1)
        self.assertIn(self.details_topic,self.mock_client.messages)
        time.sleep(2)
        _create_file()
        exp_tp = metadata_manager.experiment.measurement(experiment_id=self._adapter._interpreter.id)
        self.assertTrue(len(self.mock_client.messages.keys()) == 3)
        self.assertIn(self.start_topic,self.mock_client.messages)
        self.assertIn(self.running_topic,self.mock_client.messages)
        self.assertEqual(len(self.mock_client.messages[self.start_topic]),1)
        self.assertEqual(self.mock_client.messages[self.start_topic][0]["experiment_id"],
                         self._adapter._interpreter.id)
        self.assertEqual(len(self.mock_client.messages[self.running_topic]),1)
        self.assertTrue(self.mock_client.messages[self.running_topic][0]=="True")

        time.sleep(2)
        _modify_file()
        self.assertTrue(len(self.mock_client.messages.keys()) == 4)
        meas = self.mock_client.messages[exp_tp]
        self.assertEqual(len(meas),1)
        time.sleep(2)

        self.mock_client.reset_messages()
        _delete_file()
        time.sleep(2)
        self.assertTrue(len(self.mock_client.messages.keys()) == 2)
        self.assertEqual(len(self.mock_client.messages[self.running_topic]),1)
        self.assertTrue(self.mock_client.messages[self.running_topic][0]=="False")
        self.assertEqual(len(self.mock_client.messages[self.stop_topic]),1)
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

if __name__ == "__main__":
    unittest.main()
