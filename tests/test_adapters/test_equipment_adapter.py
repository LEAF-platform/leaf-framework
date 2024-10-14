import os
import shutil
import sys
import time
import unittest
from threading import Thread

import yaml

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))


from core.modules.output_modules.mqtt import MQTT
from core.modules.input_modules.file_watcher import FileWatcher
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.control import ControlPhase
from core.modules.process_modules.discrete_module import DiscreteProcess

from core.adapters.core_adapters.bioreactor import Bioreactor
from core.adapters.equipment_adapter import AbstractInterpreter

from mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import MetadataManager

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

class MockBioreactorInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        self.id = "test_bioreactor"

    def metadata(self,data):
        return data

    def measurement(self,data):
        return data
    
    def simulate(self):
        return
    
class MockBioreactor(Bioreactor):
    def __init__(self,instance_data,fp):
        metadata_manager = MetadataManager()
        #metadata_manager.add_equipment_data(instance_data)
        watcher = FileWatcher(fp,metadata_manager)
        output = MQTT(broker,port,username=un,password=pw,clientid=None)
        start_p = ControlPhase(output,metadata_manager.experiment.start,metadata_manager)
        stop_p = ControlPhase(output,metadata_manager.experiment.stop,metadata_manager)
        measure_p = MeasurePhase(output,metadata_manager)
        details_p = ControlPhase(output,metadata_manager.details,metadata_manager)

        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p,measure_p,stop_p]
        mock_process = [DiscreteProcess(phase)]
        super().__init__(instance_data,watcher,mock_process,
                         MockBioreactorInterpreter(),metadata_manager)

# Note the tests haven't been updated here since the rework.
class TestBioreactor(unittest.TestCase):
    def setUp(self):
        if not os.path.isfile(text_watch_file):
            with open(text_watch_file, "w"):
                pass
        
        self.instance_data = {"instance_id" : "test_biolector123",
                              "institute" : "test_ins"}
        self.mock_client = MockBioreactorClient(broker, port, username=un,password=pw)
        
        self._adapter = MockBioreactor(self.instance_data,
                                              text_watch_file)
        self._adapter._metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        
        self.details_topic = self._adapter._metadata_manager.details()
        self.start_topic = self._adapter._metadata_manager.experiment.start()
        self.stop_topic = self._adapter._metadata_manager.experiment.stop()


        self.mock_client.flush(self.details_topic)
        self.mock_client.flush(self.start_topic)
        self.mock_client.flush(self.stop_topic)

        time.sleep(2)
        self.mock_client.subscribe(self.start_topic)
        self.mock_client.subscribe(self.stop_topic)
        self.mock_client.subscribe(self.details_topic)
        time.sleep(2)
    
    def tearDown(self):
        self._adapter.stop()
        if os.path.isfile(text_watch_file):
            os.remove(text_watch_file)

    def test_details(self):
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        self.assertIn(self.details_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.details_topic]) == 1)

    def test_start(self):
        if self.start_topic in self.mock_client.messages:
            del self.mock_client.messages[self.start_topic]
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

        self.assertIn(self.start_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.start_topic]) == 1)

    def test_stop(self):
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        self.mock_client.reset_messages()
        time.sleep(2)
        _delete_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)
        self.assertIn(self.stop_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.stop_topic]) == 1)

        self.mock_client.messages = {}
        self.mock_client.unsubscribe(self.start_topic)
        self.mock_client.subscribe(self.start_topic)
        self.assertEqual(self.mock_client.messages,{})
        
    def test_update(self):
        exp_tp = self._adapter._metadata_manager.experiment.measurement(experiment_id=self._adapter._interpreter.id,
                                                                        measurement="unknown")
        self.mock_client.subscribe(exp_tp)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file()
        time.sleep(2)
        _modify_file()
        time.sleep(2)
        _delete_file()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)
        self.assertIn(exp_tp, self.mock_client.messages)   

if __name__ == "__main__":
    unittest.main()

