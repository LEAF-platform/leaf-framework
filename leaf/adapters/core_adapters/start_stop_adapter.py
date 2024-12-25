from typing import Optional
from leaf.modules.process_modules.discrete_module import DiscreteProcess
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.output_modules.output_module import OutputModule
from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter

class StartStopAdapter(EquipmentAdapter):
    def __init__(self,instance_data: dict,watcher:OutputModule,
                 output: OutputModule,interpreter:AbstractInterpreter,
                 maximum_message_size: Optional[int] = 1,
                 error_holder: Optional[ErrorHolder] = None,
                 metadata_manager:MetadataManager=None,
                 experiment_timeout:int=None):
    
        start_p = StartPhase(metadata_manager)
        stop_p = StopPhase(metadata_manager)
        measure_p = MeasurePhase(metadata_manager, 
                                 maximum_message_size=maximum_message_size)
        details_p = InitialisationPhase(metadata_manager)

        phase = [start_p, measure_p, stop_p,details_p]
        process = [DiscreteProcess(output,phase)]

        super().__init__(instance_data,watcher,process,interpreter,
                         metadata_manager=metadata_manager,
                         error_holder=error_holder,
                         experiment_timeout=experiment_timeout)