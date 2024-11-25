import time
from datetime import datetime
from typing import Any

from influxobject import InfluxPoint

from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.measurement_handler.terms import measurement_manager #??
from leaf.error_handler.exceptions import InterpreterError
from leaf.error_handler.exceptions import SeverityLevel
import re
from influxobject import InfluxPoint
from typing import Dict, Optional
from datetime import datetime

filenane = "opentrons_log.log"
exception_str = "Exception raised by protocol"
date_code_domain_pattern = (
    r"^(?P<date>[A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2}) "
    + "(?P<code>\S+) (?P<domain>\S+)$"
)

class OpentronsInterpreter(AbstractInterpreter):
    def __init__(self,error_holder=None):
        super().__init__(error_holder=error_holder)

    def metadata(self, data) -> dict[str, any]:
        pass
    
    def measurement(self, data: list[str]) -> dict[str, Any]:
        influx_objects = []

        def add_influx_object(
            action_data: Dict[str, Optional[str]],
            timestamp: Optional[str] = None,
            code: Optional[str] = None,
            domain: Optional[str] = None):
            influx_object = InfluxPoint()
            influx_object.set_measurement("Opentrons")

            for key, value in action_data.items():
                if value is not None:
                    influx_object.add_field(key, value)

            if timestamp is None:
                timestamp = datetime.now().strftime("%b %d %H:%M:%S")
            influx_object.set_timestamp(datetime.strptime(timestamp, 
                                                    "%b %d %H:%M:%S"))

            if code:
                influx_object.add_tag("Not_sure_what_code_means", code)
            if domain:
                influx_object.add_tag("domain", domain)

            influx_objects.append(influx_object)

        exceptions = []
        in_exception = False
        exception = None

        for line in data:
            line = line.strip()
            parts = re.split(r"\[(\d+)\]", line)
            if in_exception:
                if len(parts) == 3:
                    in_exception = False
                    exceptions.append(exception.copy())
                    exception = None
                    continue
                else:
                    exception.append(line)
                    continue
            elif exception_str in line:
                in_exception = True
                exception = []
                continue

            if len(parts) == 1:
                match = re.match(date_code_domain_pattern, parts[0])
                if match:
                    date = match.group("date")
                    code = match.group("code")
                    domain = match.group("domain")
                    action_data = _parse_action(parts[0][len(match.group(0)) :].strip())
                    add_influx_object(action_data, date, code, domain)
                else:
                    raise NotImplementedError()
            else:
                assert len(parts) == 3
                action = parts[2][2:]
                action_data = _parse_action(action)

                date, code, domain = None, None, None
                match = re.match(date_code_domain_pattern, parts[0])
                if match:
                    date = match.group("date")
                    code = match.group("code")
                    domain = match.group("domain")
                else:
                    raise NotImplementedError()
        return influx_objects

    def simulate(self, read_file, write_file, wait):
        with open(read_file,"r") as file:
            data = file.readlines()
        
        for line in data:
        #os.makedirs(os.path.dirname(write_file), exist_ok=True)
            with open(write_file, mode='a') as file:
                file.write(line)
                time.sleep(0.1)


def _parse_action(action: str) -> Dict[str, Optional[str]]:
    """
    Parse an action string into its components.
    
    Args:
        action (str): The action string to parse.
    
    Returns:
        Dict[str, Optional[str]]: Parsed components as a dictionary.
    """
    result = {
        "command": None,
        "instrument": None,
        "location": None,
        "volume": None,
        "rate": None,
        "repetitions": None,
        "details": None,
    }

    if action.startswith("command."):
        # Extract command type and action data
        command, action_data = action.split(":", 1)
        result["command"] = command.removeprefix("command.")
        action_data = action_data.strip()

        # Store the original data to calculate details later
        remaining_data = action_data

        # Parse instrument
        if "instrument:" in action_data:
            instrument_part = re.search(r"instrument:\s*([^,]+)", action_data)
            if instrument_part:
                result["instrument"] = instrument_part.group(1).strip()
                remaining_data = remaining_data.replace(instrument_part.group(0), "").strip()

        # Parse location
        if "location:" in action_data:
            location_part = re.search(r"location:\s*([^,]+)", action_data)
            if location_part:
                result["location"] = location_part.group(1).strip()
                remaining_data = remaining_data.replace(location_part.group(0), "").strip()

        # Parse volume
        if "volume:" in action_data:
            volume_part = re.search(r"volume:\s*([\d\.]+)", action_data)
            if volume_part:
                result["volume"] = volume_part.group(1).strip()
                remaining_data = remaining_data.replace(volume_part.group(0), "").strip()

        # Parse rate
        if "rate:" in action_data:
            rate_part = re.search(r"rate:\s*([\d\.]+)", action_data)
            if rate_part:
                result["rate"] = rate_part.group(1).strip()
                remaining_data = remaining_data.replace(rate_part.group(0), "").strip()

        # Parse repetitions (for MIX commands)
        if "repetitions:" in action_data:
            repetitions_part = re.search(r"repetitions:\s*([\d]+)", action_data)
            if repetitions_part:
                result["repetitions"] = repetitions_part.group(1).strip()
                remaining_data = remaining_data.replace(repetitions_part.group(0), "").strip()

        # Store remaining data as details
        result["details"] = remaining_data.strip() if remaining_data else None
    else:
        result["command"] = action
    return result