import os
import copy
import uuid
import csv
from datetime import datetime
import time
from typing import Any
from influxobject import InfluxPoint

from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.measurement_handler.terms import measurement_manager
from leaf.error_handler.exceptions import InterpreterError
from leaf.error_handler.exceptions import SeverityLevel

# Define wavelength ranges for different measurement types
OD_EX_RANGE = (600, 630)
OD_EM_RANGE = (600, 630)

PH_EX_RANGE = (460, 480)
PH_EM_RANGE = (520, 530)

DO_EX_RANGE = (510, 530)
DO_EM_RANGE = (590, 610)

FLUORESCENCE_EX_RANGE = (350, 580)
FLUORESCENCE_EM_RANGE = (450, 610)

WELL_NUM_KEY = "well_number"
MEASUREMENT_NAME_KEY = "measurement_name"

class Biolector1Interpreter(AbstractInterpreter):
    """
    Interpreter for Biolector1 EquipmentAdapter. Handles metadata extraction,
    measurement processing, and simulation based on Biolector1 CSV data.
    """
    def __init__(self,error_holder=None):
        super().__init__(error_holder=error_holder)
        self._TARGET_PARAMS_KEY = "target_parameters"
        self._SENSORS_KEY = "sensors"
        self._filtermap = None
        self._parameters = None
        self._sensors = None

    def _get_measurement_type(self, ex, em) -> str:
        """
        Identify the measurement type (OD, pH, DO, fluorescence) 
        based on excitation (ex) and emission (em) wavelengths.
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

    def _get_filtername(self, identifier: str) -> str:
        """
        Return the filter name associated with 
        a given number in the metadata.
        
        Args:
            identifier: The filter identifier.

        Returns:
            The name of the filter corresponding to the identifier.
        
        Raises:
            ValueError: If the identifier is not in the filter map.
        """
        if self._filtermap is None:
            return None
        if identifier not in self._filtermap:
            self._handle_exception(InterpreterError(f'{identifier} not a valid filter code'))
            return None
        return self._filtermap[identifier]

    def _get_sensor_data(self, name):
        """
        Retrieve sensor data given its name.

        Args:
            name: The name of the sensor.

        Returns:
            A dictionary containing sensor data.

        Raises:
            ValueError: If the sensor name is not found.
        """
        if name not in self._sensors:
            self._handle_exception(InterpreterError(f'{name} not a valid filter code.'))
            return None
        return self._sensors[name]

    def metadata(self, data) -> dict[str, any]:
        """
        Parse metadata from the Biolector1 initial CSV data, building a 
        metadata payload from details such as protocol, device, 
        user, and filtersets. Also sets some 
        member variables for when measurements are taken.
        
        Args:
            data: The initial data given by the CSVWatcher.

        Returns:
            A dictionary containing metadata for the experiment, 
            including the experiment ID,target parameters, and sensors.
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
        if not isinstance(data,list):
            self._handle_exception(InterpreterError(f'Cant extract metadata, input malformed'))
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
        self._filtermap = {k: v["FILTERNAME"] for k, v in filtersets.items()}
        self._parameters = parameters
        self._sensors = {v.pop('FILTERNAME'): v for v in copy.deepcopy(filtersets).values()}

        payload = {
            self.TIMESTAMP_KEY: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            self.EXPERIMENT_ID_KEY: self.id
        }
        if self._parameters is not None:
            payload[self._TARGET_PARAMS_KEY] = self._parameters
        if self._sensors is not None:
            payload[self._SENSORS_KEY] = self._sensors
        return payload
    
    def measurement(self, data: list[str]) -> dict[str, Any]|None:
        """
        Process measurement data from the Biolector1 CSV. 
        Generates a dictionary containing transformed measurement values 
        for different parameters.
        
        Args:
            data: The current data given by the CSVWatcher.

        Returns:
            A dictionary update containing transformed measurement data.
        """
        if data[-1][0] == "READING":
            return None
        data = data[::-1]
        influx_objects = []
        if data[0][0] == "R":
            data = data[1:]
        reading = data[0][0]

        if self._filtermap is None:
            self._handle_exception(InterpreterError("No filters defined, " 
                                                     "likely because the "
                                                     "adapter hasn't identified "
                                                     "experiment start",
                                                     severity=SeverityLevel.WARNING))
        
        for row in data:            
            if len(row) == 0 or row[0] == "R":
                continue
            if row[0] != reading:
                return [i.to_json() for i in influx_objects]

            fs_code = int(row[4])
            name = self._get_filtername(fs_code)
            well_num = row[1]
            ip = InfluxPoint()
            ip.add_tag(WELL_NUM_KEY,well_num)
            ip.set_measurement("Biolector")
            ip.set_timestamp(datetime.now())
            influx_objects.append(ip) 

            amplitude = row[5]
            if name is not None:
                sensor_data = self._get_sensor_data(name)
                excitation = int(sensor_data["EX [nm]"])
                emitence = int(sensor_data["EM [nm]"])
                measurement = self._get_measurement_type(excitation, emitence)
                measurement_term = measurement.term
                value = measurement.transform(amplitude)
                assert(measurement_term not in ip.fields)
                ip.add_tag(MEASUREMENT_NAME_KEY,name)
                ip.add_field(measurement_term,value)
            else:
                excp = InterpreterError(f'Cant identify measurement name')
                self._handle_exception(excp)
                value = amplitude
                measurement_term = "unknown_measurement"
                cur_measurement_term = measurement_term
                index = 1
                while cur_measurement_term not in ip.fields:
                    cur_measurement_term = measurement_term + str(index)
                    index +=1
                ip.add_field(cur_measurement_term,value)

        return [i.to_json() for i in influx_objects]

    def simulate(self, read_file, write_file, wait):
        """
        Simulate the Biolector1 process by reading 
        chunks of data from an input file and writing it to 
        an output file with delays between writes.
        
        Args:
            read_file: The input CSV file to read data from.
            write_file: The output file where simulated data is written.
            wait: Time (in seconds) to wait between writing chunks of data.
        """
        def write(chunk):
            os.makedirs(os.path.dirname(write_file), exist_ok=True)
            with open(write_file, mode='a', newline='', encoding='latin-1') as file:
                writer = csv.writer(file, delimiter=';')
                writer.writerows(chunk)

        with open(read_file, 'r', encoding='latin-1') as file:
            reader = csv.reader(file, delimiter=';')
            rows = list(reader)
        
        for index, row in enumerate(rows):
            if len(row) == 0:
                continue
            if row[0] == "READING":
                metadata = rows[:index + 1]
                data = rows[index + 1:]
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
