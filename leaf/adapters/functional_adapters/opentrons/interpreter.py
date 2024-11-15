from datetime import datetime
from typing import Any

from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.measurement_terms.manager import measurement_manager
from leaf.error_handler.exceptions import InterpreterError
from leaf.error_handler.exceptions import SeverityLevel

class OpentronsInterpreter(AbstractInterpreter):
    def __init__(self,error_holder=None):
        super().__init__(error_holder=error_holder)

    def metadata(self, data) -> dict[str, any]:
        pass
    
    def measurement(self, data: list[str]) -> dict[str, Any]|None:
        measurements: dict[str, list[dict[str, Any]]] = {}
        update = {
            "measurement": "Opentrons",
            "tags": {"project": "opentrons"},
            "fields": measurements,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return update

    def simulate(self, read_file, write_file, wait):
        pass
