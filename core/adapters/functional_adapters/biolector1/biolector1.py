import os
from threading import Thread
import time

# Bioreactor
from core.adapters.core_adapters.bioreactor import Bioreactor
from core.adapters.functional_adapters.biolector1.biolector1_interpreter import Biolector1Interpreter
# Processes
from core.modules.process_modules.discrete_module import DiscreteProcess
# Phases
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.initialisation import InitialisationPhase
# Watcher
from core.modules.input_modules.csv_watcher import CSVWatcher

from core.metadata_manager.metadata import MetadataManager
# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'biolector1.json')


class Biolector1Adapter(Bioreactor):
    def __init__(self,instance_data,output,write_file=None):
        metadata_manager = MetadataManager()
        watcher = CSVWatcher(write_file,metadata_manager)
        start_p = StartPhase(output,metadata_manager)
        stop_p = StopPhase(output,metadata_manager)
        measure_p = MeasurePhase(output,metadata_manager)
        details_p = InitialisationPhase(output,metadata_manager)

        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p,measure_p,stop_p]
        process = [DiscreteProcess(phase)]
        super().__init__(instance_data,watcher,process,
                         Biolector1Interpreter(),
                         metadata_manager=metadata_manager)
        self._write_file = write_file
        self._metadata_manager.add_equipment_data(metadata_fn)


    def simulate(self,filepath,wait=None,delay=None):
        if wait is None:
            wait = 10

        if os.path.isfile(self._write_file):
            raise ValueError("Trying to run test when the file exists.")
        
        proxy_thread = Thread(target=self.start)
        proxy_thread.start()
        if delay is not None:
            print(f'Delay for {delay} seconds.')
            time.sleep(delay)
            print("Delay finished.")

        self._interpreter.simulate(filepath,self._write_file,wait)
        time.sleep(wait)
        os.remove(self._write_file)

        self.stop()
        proxy_thread.join()

