import os
import copy
import uuid
import csv
from datetime import datetime
import time

from core.adapters.equipment_adapter import AbstractInterpreter

from core.measurement_terms.manager import measurement_manager

# Define the wavelength ranges for different measurement types
# II have no idea if these could be generalised for 
# all bioreactors/equipment that use a wavelength measurement approach.
# https://www.m2p-labs.com/fileadmin/redakteur/Data_sheets/Filter_List/Filter-Modules_BioLector_I.pdf

OD_EX_RANGE = (600, 630)
OD_EM_RANGE = (600, 630)

PH_EX_RANGE = (460, 480)
PH_EM_RANGE = (520, 530)

DO_EX_RANGE = (510, 530)
DO_EM_RANGE = (590, 610)

FLUORESCENCE_EX_RANGE = (350, 580)
FLUORESCENCE_EM_RANGE = (450, 610)

class Biolector1Interpreter(AbstractInterpreter):
    def __init__(self):
        super().__init__()
        self._TARGET_PARAMS_KEY = "target_parameters"
        self._SENSORS_KEY = "sensors"
        self._filtermap = None
        self._parameters = None
        self._sensors = None

    def _get_measurement_type(self,ex, em):
        """
        Takes EX (excitation) and EM (emission) wavelengths 
        and returns the measurement type
        based on approximate wavelength ranges.
        """
        if (OD_EX_RANGE[0] <= ex <= OD_EX_RANGE[1] and 
            OD_EM_RANGE[0] <= em <= OD_EM_RANGE[1]):
            return measurement_manager.OD
        elif (PH_EX_RANGE[0] <= ex <= PH_EX_RANGE[1] and 
            PH_EM_RANGE[0] <= em <= PH_EM_RANGE[1]):
            return measurement_manager.pH
        elif (DO_EX_RANGE[0] <= ex <= DO_EX_RANGE[1] and 
            DO_EM_RANGE[0] <= em <= DO_EM_RANGE[1]):
            return measurement_manager.DO
        elif (FLUORESCENCE_EX_RANGE[0] <= ex <= FLUORESCENCE_EX_RANGE[1] and 
                FLUORESCENCE_EM_RANGE[0] <= em <= FLUORESCENCE_EM_RANGE[1]):
            return measurement_manager.fluorescence
        else:
            return 'Unknown Measurement'
    
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
            payload[self._TARGET_PARAMS_KEY] = self._parameters
        if  self._sensors is not None:
            payload[self._SENSORS_KEY] = self._sensors
        return payload
    
    def measurement(self,data):        
        # The file is created with content and 
        # therefore update is called.
        # Dont want to do anything
        if data[-1][0] == "READING":
            return None
        data = data[::-1]
        measurements = {}
        update = {"measurement" : "Biolector1",
                  "tags": {"project": "indpensim"},
                  "fields" : measurements,
                  "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                  }
        
        if data[0][0] == "R":
            data = data[1:]
        reading = data[0][0]
        for row in data:
            if len(row) == 0:
                continue
            if row[0] == "R":
                continue
            if row[0] != reading:
                return update
            fs_code = int(row[4])


            name = self._get_filtername(fs_code)
            sensor_data = self._get_sensor_data(name)
            excitation = int(sensor_data["EX [nm]"])
            emitence = int(sensor_data["EM [nm]"])
            measurement = self._get_measurement_type(excitation,emitence)
            well_num = row[1]
            amplitude = row[5]

            if measurement.term not in measurements:
                measurements[measurement.term] = []
            
            '''
            Keep these out bc dont really know how to make a final "Value"
            phase = row[6]
            gain = sensor_data["GAIN"]
            '''
            value = measurement.transform(amplitude)
            measurement_data = {"value" : value,
                                "name":name,
                                "well_num":well_num}
            measurements[measurement.term].append(measurement_data)

        return update

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