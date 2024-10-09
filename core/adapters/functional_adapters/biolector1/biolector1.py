import os
import copy
import uuid
import csv
from datetime import datetime
from threading import Thread
import time

# Bioreactor
from core.adapters.core_adapters.bioreactor import Bioreactor
from core.adapters.equipment_adapter import AbstractInterpreter
# Processes
from core.modules.process_modules.discrete_module import DiscreteProcess
# Phases
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.phase_modules.measurement import MeasurementPhase
from core.modules.phase_modules.initialisation import InitialisationPhase
# Watcher
from core.modules.input_modules.csv_watcher import CSVWatcher
# Measurements
from core.modules.measurement_modules.biomass import Biomass
from core.modules.measurement_modules.o2 import O2
from core.modules.measurement_modules.ph import pH

from core.metadata_manager.metadata import MetadataManager
# Note the biolector json file is an example, not a concrete decision on terms...
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'biolector1.json')

class Biolector1Interpreter(AbstractInterpreter):
    def __init__(self):
        super().__init__()
        self._filtermap = None
        self._parameters = None
        self._sensors = None

    def _get_filtername(self,identifier):
        if identifier not in self._filtermap:
            raise ValueError(f'{identifier} not a valid filter code.')
        return self._filtermap[identifier]
    
    def _get_sensor_data(self,name):
        if name not in self._sensors:
            raise ValueError(f'{name} not a valid sensor name.')
        return self._sensors[name]
    
    def metadata(self,data):
        """
        Extracts data necessary to encode in start payload.
        It's a dirty function but necessary because the Biolector1 CSV data is 
        VERY poorly structured.
        """
        FILTERSET_ID_IDX = 0
        FILTERNAME_IDX = 1
        EX_IDX = 2
        EM_IDX = 3
        LAYOUT_IDX = 4
        FILTERNR_IDX = 5
        GAIN_IDX = 6
        PHASESTATISTICSSIGMA_IDX = 7
        SIGNALQUALITYTOLERANCE_IDX = 8
        REFERENCE_VALUE_IDX = 9
        EM2_IDX = 10
        GAIN2_IDX = 11
        PROCESS_PARAM_IDX = 12
        PROCESS_VALUE_IDX = 13

        filtersets = {}
        parameters = {}
        md = {'PROTOCOL': '', 'DEVICE': '', 'USER': '', 'COMMENT': ''}
        in_filtersets = False

        for row in data:
            if not row or not row[0]:
                continue
            if row[0] == 'PROTOCOL':
                md['PROTOCOL'] = row[1]
            elif row[0] == 'DEVICE':
                md['DEVICE'] = row[1]
            elif row[0] == 'USER':
                md['USER'] = row[1]
                md['COMMENT'] = ' '.join(row[3:]).strip() if len(row) > 3 else ''
            elif row[0] == 'FILTERSET':
                in_filtersets = True
                continue
            elif row[0] == 'READING':
                in_filtersets = False
                continue

            if in_filtersets and row[FILTERSET_ID_IDX].strip().isdigit():
                filterset_id = int(row[FILTERSET_ID_IDX].strip())
                filtersets[filterset_id] = {
                    'FILTERNAME': row[FILTERNAME_IDX],
                    'EX [nm]': row[EX_IDX],
                    'EM [nm]': row[EM_IDX],
                    'LAYOUT': row[LAYOUT_IDX],
                    'FILTERNR': row[FILTERNR_IDX],
                    'GAIN': row[GAIN_IDX],
                    'PHASESTATISTICSSIGMA': row[PHASESTATISTICSSIGMA_IDX],
                    'SIGNALQUALITYTOLERANCE': row[SIGNALQUALITYTOLERANCE_IDX],
                    'REFERENCE VALUE': row[REFERENCE_VALUE_IDX],
                    'EM2 [nm]': row[EM2_IDX],
                    'GAIN2': row[GAIN2_IDX]
                }

                if len(row) > PROCESS_PARAM_IDX and row[PROCESS_PARAM_IDX].startswith('SET '):
                    param_name = row[PROCESS_PARAM_IDX].strip()
                    param_value = row[PROCESS_VALUE_IDX].strip() if len(row) > PROCESS_VALUE_IDX else ''
                    parameters[param_name] = param_value

        self.id = f'{md["PROTOCOL"]}-{md["DEVICE"]}-{md["USER"]}-{str(uuid.uuid4())}'
        self._filtermap = {k:v["FILTERNAME"] for k,v in filtersets.items()}
        self._parameters = parameters
        self._sensors = {v.pop('FILTERNAME'): v for v in 
                        copy.deepcopy(filtersets).values()}
        payload = {
        self.TIMESTAMP_KEY : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        self.EXPERIMENT_ID_KEY : self.id}
        if self._parameters is not None:
            payload[self.TARGET_PARAMS_KEY] = self._parameters
        if  self._sensors is not None:
            payload[self.SENSORS_KEY] = self._sensors
        return payload
    
    def measurement(self,data,measurements):        
        # The file is created with content and 
        # therefore update is called.
        # Dont want to do anything
        if data[-1][0] == "READING":
            return None
        data = data[::-1]
        update = {}
        if data[0][0] == "R":
            data = data[1:]
        reading = data[0][0]
        for row in data:
            if len(row) == 0:
                continue
            if row[0] == "R":
                continue
            if row[0] != reading:
                return {self.TIMESTAMP_KEY : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        self.UPDATE_KEY : update}
            fs_code = int(row[4])
            name = self._get_filtername(fs_code)
            if name not in update:
                update[name] = {}
            well_num = row[1]
            update[name][well_num] = {}
            amplitude = row[5]
            phase = row[6]
            sensor_data = self._get_sensor_data(name)
            excitation = sensor_data["EX [nm]"]
            emitence = sensor_data["EM [nm]"]
            gain = sensor_data["GAIN"]
            value = amplitude
            update[name][well_num]["value"] = value
        return {self.TIMESTAMP_KEY : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                self.UPDATE_KEY : update}

    def simulate(self,read_file,write_file,wait):
        def write(chunk):
            with open(write_file, mode='a', newline='', encoding='latin-1') as file:
                writer = csv.writer(file, delimiter=';')
                writer.writerows(chunk)

        with open(read_file, 'r', encoding='latin-1') as file:
            reader = csv.reader(file, delimiter=';')
            rows = list(reader)
        for index,row in enumerate(rows):
            if len(row) == 0:
                continue
            if row[0] == "READING":
                metadata = rows[:index+1]
                data = rows[index+1:]
                break
        write(metadata)
        time.sleep(wait)
        chunk = [data.pop(0)]
        cur_read = data[0][0]
        for row in data:
            if row[0] != cur_read and row[0] != "R":
                write(chunk)
                chunk = []
                cur_read = row[0]
                time.sleep(wait)
            else:
                chunk.append(row)
        write(chunk)

interpreter = Biolector1Interpreter()

class Biolector1Adapter(Bioreactor):
    def __init__(self,instance_data,output,write_file=None):
        metadata_manager = MetadataManager()
        watcher = CSVWatcher(write_file,metadata_manager)
        measurements = [Biomass(),O2(),pH()]
        start_p = StartPhase(output,metadata_manager)
        stop_p = StopPhase(output,metadata_manager)
        measure_p = MeasurementPhase(output,measurements,metadata_manager)
        details_p = InitialisationPhase(output,metadata_manager)

        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p,measure_p,stop_p]
        mock_process = [DiscreteProcess(phase)]
        super().__init__(instance_data,watcher,mock_process,
                         interpreter,metadata_manager=metadata_manager)
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

        interpreter.simulate(filepath,self._write_file,wait)
        time.sleep(wait)
        os.remove(self._write_file)

        self.stop()
        proxy_thread.join()

