import os
from threading import Thread
import time
from typing import Optional

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.adapters.functional_adapters.opentrons.interpreter import OpentronsInterpreter
from leaf.modules.input_modules.file_watcher import FileWatcher
from leaf.metadata_manager.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.output_modules.output_module import OutputModule

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "device.json")

'''
Things that need to be figured out:
1. If we can watch a log file. They appear to be some sort of binary file so its not clear if a modified filewatcher can be achieved.
    If not, then we will need to use the journalctl approach which will then need its new class and implementation.
    Whatever the mechanism ends up being, need to add the ability for multiple watch (opentrons,opentrons-serial and the last one which i cant remember...)
2. How general is the opentrons log files. Need to disseminate what the actual contents are.
3. What will the start acutally look like? Can any metadata acutally be derived and how will we know if an experiment has started? 
    (This second point will depend on file OR journal)

TODO
1. Try make the _parse_action a little more granular/robust.
2. Check out none implemented action types.
'''

class OpentronsAdapter(StartStopAdapter):
    """
    Adapter class for opentrons liquid handling robot, discrete(?) equipment that 
    moves liquid via pipette around a plane.

    The opentrons adapter is different from most adapters as it doesnt 
    measure anything, instead it performs a set of preprogrammed steps.
    Therefore, this adapter aims to transmit these steps rahter than measurements.
    """
    def __init__(self,instance_data: dict,output: OutputModule,
                 maximum_message_size: Optional[int] = 1, 
                 error_holder: Optional[ErrorHolder] = None):
        """
        Initialise OpentronsAdapter, setting up phases, process adapters, and metadata.

        Args:
            instance_data: Data specific to this instance.
            output: The OutputModule responsible for handling and transmitting data.
            write_file: ??
            maximum_message_size: Sets the maximum number of messages send in a single payload.
        """
        
        metadata_manager = MetadataManager()
        # Its unsure what this mechanism will be.
        # It will either be filewatching or using this journalctl approach.
        self._write_file = "test/test.test"
        # This last_line appraoch probably wont work. Maybe some other method 
        # where we can only get the difference would be useful.
        # Keep track of last line or if there is some other way...
        watcher = FileWatcher(self._write_file, metadata_manager,
                              last_line=True)

        interpreter = OpentronsInterpreter(error_holder=error_holder)
        super().__init__(instance_data,watcher,output,interpreter,
                         maximum_message_size=maximum_message_size,
                         error_holder=error_holder,
                         metadata_manager=metadata_manager)
        
        self._metadata_manager.add_equipment_data(metadata_fn)

    def simulate(self, filepath: str, wait: Optional[int] = None, 
                 delay: Optional[int] = None) -> None:
        """
        Simulate/Mock an experiment within the Opentrons using existing data.

        Args:
            filepath: Path to the file that provides input data.
            wait: Time (in seconds) to wait between measurements
            delay: Optional delay (in seconds) before starting the simulation.

        Raises:
            ValueError: If the write file already exists, to prevent overwriting.
        """
        if wait is None:
            wait = 10

        # Unclear wether this will be needed...
        '''
        if os.path.isfile(self._write_file):
            exception = AdapterLogicError(
                "Trying to run test when the file exists.",
                severity=SeverityLevel.CRITICAL,
            )
            if self._error_holder is not None:
                self._error_holder.add_error(exception)
            else:
                raise exception
        '''

        proxy_thread = Thread(target=self.start)
        proxy_thread.start()

        if delay is not None:
            print(f"Delay for {delay} seconds.")
            time.sleep(delay)
            print("Delay finished.")

        self._interpreter.simulate(filepath, self._write_file, wait)
        time.sleep(wait)

        os.remove(self._write_file)

        self.stop()
        proxy_thread.join()
