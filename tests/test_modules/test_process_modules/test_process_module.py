import os
import sys
import unittest
from threading import Thread
import yaml
import time
import shutil
sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))


from core.modules.output_modules.mqtt import MQTT
from core.modules.input_modules.file_watcher import FileWatcher
from core.modules.measurement_modules.biomass import Biomass
from core.modules.measurement_modules.o2 import O2
from core.modules.measurement_modules.ph import pH
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.control import ControlPhase
from core.modules.process_modules.continous_module import ContinousProcess
from core.modules.process_modules.discrete_module import DiscreteProcess
from mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import metadata_manager

with open('../../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)
broker = config["OUTPUT"]["broker_address"]
port = int(config["OUTPUT"]["port"])
un = config["OUTPUT"]["username"]
pw = config["OUTPUT"]["password"]

curr_dir = os.path.dirname(os.path.realpath(__file__))
test_file_dir = os.path.join(curr_dir,"..","static_files")
test_file = os.path.join(test_file_dir,"ecoli-GFP-mCherry_inter.csv")
initial_file = os.path.join(test_file_dir,"biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir,"biolector1_measurement.csv")
text_watch_file = os.path.join("tmp.csv")
output = MQTT(broker,port,username=un,password=pw)

measurements = [Biomass(),O2(),pH()]

mock_client = MockBioreactorClient(broker,port,username=un,password=pw)
mock_client.subscribe("#")

metadata_manager._metadata["equipment"] = {}
metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"

def _create_file():
    shutil.copyfile(initial_file, text_watch_file)
    time.sleep(2)

def _modify_file():
    with open(measurement_file, 'r') as src:
        content = src.read()
    with open(text_watch_file, 'a') as dest:
        dest.write(content)
    time.sleep(2)

def _delete_file():
    if os.path.isfile(text_watch_file):
        os.remove(text_watch_file)

def _run_change(func):
    mthread = Thread(target=func)
    mthread.start()
    mthread.join()

class TestContinousProcess(unittest.TestCase):
    def setUp(self):
        self.watcher = FileWatcher(text_watch_file)
        self._phase = MeasurePhase(output,
                                    metadata_manager.experiment.measurement,
                                    measurements)
        self._module = ContinousProcess(self._phase)
        self._phase.set_interpreter(None)
        self._mock_experiment="test_experiment_id"
        self.watcher.add_measurement_callback(self._mock_update)

    def tearDown(self):
        self.watcher.stop
        time.sleep(2)
        if os.path.isfile(text_watch_file):
            os.remove(text_watch_file)

    def _mock_update(self,data):
        self._phase.update(experiment_id=self._mock_experiment,
                             data=data)
        
    def test_continous_process(self):
        _run_change(_create_file)
        self.watcher.start()
        time.sleep(2)
        _run_change(_modify_file)
        time.sleep(2)
        for k,v in mock_client.messages.items():
            if metadata_manager.experiment.measurement(experiment_id=self._mock_experiment)== k:
                break
        else:
            self.fail()


class TestDiscreteProcess(unittest.TestCase):
    def setUp(self):
        self.watcher = FileWatcher(text_watch_file)
        start_p = ControlPhase(output,
                                      metadata_manager.experiment.start)
        stop_p = ControlPhase(output,
                                     metadata_manager.experiment.stop)
        self._measure_p = MeasurePhase(output,
                                        metadata_manager.experiment.measurement,
                                        measurements)
        
        phase = [start_p,self._measure_p,stop_p]
        self.watcher.add_measurement_callback(self._mock_update)
        self.watcher.add_start_callback(start_p.update)
        self.watcher.add_stop_callback(stop_p.update)
        self._module = DiscreteProcess(phase)
        self._mock_experiment="test_experiment_id"
        self._module.set_interpreter(None)

    def tearDown(self):
        self.watcher.stop()
        time.sleep(2)
        if os.path.isfile(text_watch_file):
            os.remove(text_watch_file)

    def _mock_update(self,data):
        self._measure_p.update(experiment_id=self._mock_experiment,
                             data=data)
        
    def test_discrete_process(self):
        self.watcher.start()
        time.sleep(2)
        _run_change(_create_file)
        time.sleep(2)
        _run_change(_modify_file)
        time.sleep(2)
        _run_change(_delete_file)
        time.sleep(2)

        for k,v in mock_client.messages.items():
            if metadata_manager.experiment.start() == k:
                break
        else:
            self.fail()

        for k,v in mock_client.messages.items():
            if metadata_manager.experiment.measurement(experiment_id=self._mock_experiment) == k:
                break
        else:
            self.fail()

        for k,v in mock_client.messages.items():
            if metadata_manager.experiment.stop() == k:
                break
        else:
            self.fail()
