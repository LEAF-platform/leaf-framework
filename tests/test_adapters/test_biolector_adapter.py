import os
import shutil
import sys
import time
import unittest
from threading import Thread
import yaml
import csv

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from core.adapters.functional_adapters.biolector1.biolector1 import Biolector1Adapter
from core.adapters.functional_adapters.biolector1.biolector1_interpreter import Biolector1Interpreter
from core.modules.output_modules.mqtt import MQTT
from mock_mqtt_client import MockBioreactorClient
from core.measurement_terms.manager import measurement_manager

import logging

logging.basicConfig(level=logging.DEBUG)

curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir,"..","test_config.yaml"), 'r') as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except:
    un = None
    pw = None

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

class TestBiolector1Interpreter(unittest.TestCase):
    def setUp(self):
        self._interpreter = Biolector1Interpreter()

    def _metadata_run(self):
        with open(initial_file, 'r', encoding='latin-1') as file:
            data = list(csv.reader(file, delimiter=";"))  
        return self._interpreter.metadata(data)

    def test_metadata(self):
        result = self._metadata_run()
        self.assertIn("experiment_id",result)
        self.assertIn("sensors",result)

    def test_measurement(self):
        result = self._metadata_run()
        names = list(result["sensors"].keys())
        measurement_terms = measurement_manager.get_measurements()
        with open(measurement_file, 'r', encoding='latin-1') as file:
            data = list(csv.reader(file, delimiter=";"))  
        metadata,result = self._interpreter.measurement(data)
        for measurement,measurements in result.items():
            self.assertIn(measurement,measurement_terms)
            for data in measurements:
                self.assertIn(data["name"],names)

    def test_simulate(self):
        pass

class TestBiolector1(unittest.TestCase):
    
    def setUp(self):
        if os.path.isfile(watch_file):
            os.remove(watch_file)

        self.mock_client = MockBioreactorClient(broker, port,username=un,password=pw)
        logging.debug(f"Broker: {broker} Port: {port} Username: {un}")
        self.output = MQTT(broker,port,username=un,password=pw)
        self.instance_data = {"instance_id" : "test_biolector123","institute" : "test_ins"}
        self._adapter = Biolector1Adapter(self.instance_data,
                                          self.output,
                                          watch_file)
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

    def tearDown(self):
        self._adapter.stop()
        self._flush_topics()
        self.mock_client.reset_messages()

    def _get_measurements_run(self):
        with open(initial_file, 'r', encoding='latin-1') as file:
            data = list(csv.reader(file, delimiter=";"))  
        self._adapter._interpreter.metadata(data)
        with open(measurement_file, 'r', encoding='latin-1') as file:
            data = list(csv.reader(file, delimiter=";"))  
        return self._adapter._interpreter.measurement(data)
    
    def _flush_topics(self):
        self.mock_client.flush(self.details_topic)
        self.mock_client.flush(self.start_topic)
        self.mock_client.flush(self.stop_topic)
        self.mock_client.flush(self.running_topic)

    def test_details(self):
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
        for k,v in self.instance_data.items():
            self.assertIn(k,details_data)
            self.assertEqual(v,details_data[k])
        self._flush_topics()
        self.mock_client.reset_messages()

    def test_start(self):
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
        self.assertIn(self._adapter._interpreter.id, self.mock_client.messages[self.start_topic][0]["experiment_id"])
        self.assertIn("timestamp", self.mock_client.messages[self.start_topic][0])

        self.assertIn(self.running_topic, self.mock_client.messages)
        expected_run = "True"
        self.assertEqual(self.mock_client.messages[self.running_topic][0], expected_run)

        os.remove(watch_file)
        self._flush_topics()
        self.mock_client.reset_messages()
    
    def test_stop(self):
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
        self.assertEqual(self.mock_client.messages,{})

        self._flush_topics()
        self.mock_client.reset_messages()

    def test_running(self):
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

    def test_update(self):
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

        actual_mes = self._get_measurements_run()[1]
        expected_measurements = ["Biomass",
                                    "GFP",
                                    "mCherrry/RFPII",
                                    "pH-hc",
                                    "pO2-hc"]
        seens = []
        for topic in self.mock_client.messages.keys():
            pot_mes = topic.split("/")[-1]
            exp_tp = self._adapter._metadata_manager.experiment.measurement(experiment_id=experiment_id,
                                                                            measurement=pot_mes)
            if exp_tp in topic:
                data = self.mock_client.messages[exp_tp]
                self.assertTrue(len(data),1)
                for md,measurement_data in data:
                    self.assertIn("timestamp",md)
                    measurement_type = topic.split("/")[-1]
                    self.assertIn(measurement_type,actual_mes)
                    for am in actual_mes[measurement_type]:
                        self.assertIn("value",am)
                        if am == measurement_data:
                            break
                    else:
                        self.fail()
                    name = measurement_data["name"]

                    self.assertIn(name,expected_measurements)
                    if name not in seens:
                        seens.append(name)

        self.assertCountEqual(seens,expected_measurements)
        self._flush_topics()
        self.mock_client.reset_messages() 

    def test_logic(self):
        self._flush_topics()
        self.mock_client.reset_messages()

        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self.assertTrue(len(self.mock_client.messages.keys()) == 1)
        self.assertIn(self.details_topic,self.mock_client.messages)
        time.sleep(2)
        _create_file()
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
        self.assertTrue(len(self.mock_client.messages.keys()) == 7)
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

        self._flush_topics()
        self.mock_client.reset_messages()

if __name__ == "__main__":
    unittest.main()
