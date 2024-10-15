import gzip
import logging
import os
import time
from datetime import datetime, timedelta
import uuid
from threading import Thread
import pandas as pd


from core.adapters.equipment_adapter import AbstractInterpreter, EquipmentAdapter
from core.metadata_manager.metadata import MetadataManager
from core.modules.input_modules.csv_watcher import CSVWatcher
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.phase_modules.initialisation import InitialisationPhase
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.start import StartPhase
from core.modules.phase_modules.stop import StopPhase
from core.modules.process_modules.discrete_module import DiscreteProcess

from core.measurement_terms.manager import measurement_manager

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)


current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "indpensim.json")


measurement_map = {"pH(pH:pH)" : measurement_manager.pH} # And so on...

class IndPenSimInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        logger.info("Initializing IndPenSimInterpreter")
        self._start_time = datetime.now()
        self._measurement_headings = None

    def metadata(self, data: str) -> dict[str, str]:
        self.id = f"{str(uuid.uuid4())}"
        self._start_time = datetime.now()
        if data != {}:
            self._measurement_headings = data[0][0].split(',')
        else:
            self._measurement_headings = None
        payload = {
            self.TIMESTAMP_KEY: self._start_time.strftime("%Y-%m-%d %H:%M:%S"),
            self.EXPERIMENT_ID_KEY: self.id,
            self.MEASUREMENT_HEADING_KEY: self._measurement_headings
        }
        return payload
    
    def measurement(self, data):
        cur_measurement = data[-1][0]
        if isinstance(cur_measurement, str):
            cur_measurement = cur_measurement.split(',')

        measurements = {}
        update = {'tags' : {"project":"indpensim"},
                  'measurement': 'indpensim',
                  "fields" : measurements,
                  "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                  }
        for index, measurement in enumerate(cur_measurement):
            measurement_name = self._measurement_headings[index]
            if measurement_name.isdigit():
                continue
            
            if measurement_name in measurement_map:
                measure_obj = measurement_map[measurement_name]
                measurement = measure_obj.transform(measurement)
                measurement_name = measure_obj.term

            measurements[measurement_name] = measurement
        return update
    
    def simulate(self) -> None:
        logger.error("Simulating IndPenSimInterpreter")
        print("Doing something D?")


class IndPenSimAdapter(EquipmentAdapter):
    def __init__(self, instance_data, output, write_file=None,stagger_transmit=False) -> None:
        logger.info(
            f"Initializing IndPenSimAdapter with instance data {instance_data} and output {output} and write file {write_file}"
        )
        metadata_manager: MetadataManager = MetadataManager()
        # Create a CSV watcher for the write file
        watcher: CSVWatcher = CSVWatcher(write_file, metadata_manager)
        start_p: StartPhase = StartPhase(output, metadata_manager)
        stop_p: StopPhase = StopPhase(output, metadata_manager)
        measure_p: MeasurePhase = MeasurePhase(output, metadata_manager,
                                               stagger_transmit=stagger_transmit)
        details_p: InitialisationPhase = InitialisationPhase(output, metadata_manager)
        logger.info(f"Instance data: {instance_data}")
        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        process = [DiscreteProcess(phase)]
        super().__init__(
            instance_data=instance_data,
            watcher=watcher,
            process_adapters=process,
            interpreter=IndPenSimInterpreter(),
            metadata_manager=metadata_manager,
        )
        self._write_file = write_file
        self._metadata_manager.add_equipment_data(metadata_fn)

    def simulate(self, filepath: str, wait: int = 0, delay: int = 0) -> None:
        logger.info(
            f"Simulating nothing yet for {self.instance_id} at {self.institute} with input file {filepath} and wait {wait} and delay {delay}"
        )
        proxy_thread = Thread(target=self.start)
        proxy_thread.start()

        # Read the big file and push the data to the "file watcher"?
        with gzip.open(filepath, "r") as f:
            for index, lineb in enumerate(f):
                if index == 0:
                    if os.path.isfile(self._write_file):
                        logger.warning(
                            f"Trying to run test when the file exists at {self._write_file}"
                        )
                        # Remove the file
                        os.remove(self._write_file)
                line: str = lineb.decode("utf-8")
                # Change the time column to a datetime object with the start date
                if index > 0:
                    line_split = line.split(",")
                    # Get the time
                    time_h = float(line_split[0])
                    # Convert to a datetime object
                    new_time = self._start_datetime + timedelta(hours=time_h)
                    # Replace the time column
                    line_split[0] = new_time.strftime("%Y-%m-%d %H:%M:%S")
                    line = ",".join(line_split)

                with open(self._write_file, "a") as write_file:
                    write_file.write(line)
                    logger.info(f"Writing line {index} to {self._write_file}")
                time.sleep(wait)


