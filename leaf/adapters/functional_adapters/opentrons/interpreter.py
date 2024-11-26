import time
from datetime import datetime
import uuid
from influxobject import InfluxPoint

from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.measurement_handler.terms import measurement_manager
from leaf.error_handler.exceptions import InterpreterError
import re
from influxobject import InfluxPoint
from typing import Dict, Optional
from datetime import datetime

exception_str = "Exception raised by protocol"
date_code_domain_pattern = (
    r"^(?P<date>[A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2}) "
    + "(?P<code>\S+) (?P<domain>\S+)$"
)

class OpentronsInterpreter(AbstractInterpreter):
    def __init__(self,error_holder=None):
        super().__init__(error_holder=error_holder)

    def metadata(self, data) -> dict[str, any]:
        self.id = f'{str(uuid.uuid4())}'
        payload = {
            self.TIMESTAMP_KEY: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            self.EXPERIMENT_ID_KEY: self.id
        }
        return payload
    
    def measurement(self, data: list[str]):
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
            try:
                influx_object.to_json()
            except ValueError:

                raise ValueError(line)

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
                part = parts[0]
                match = re.match(date_code_domain_pattern, part)
                if match:
                    date = match.group("date")
                    code = match.group("code")
                    domain = match.group("domain")
                    action_data = _parse_action(parts[0][len(date) :].strip())
                    add_influx_object(action_data, date, code, domain)
                else:
                    if part.startswith("--"):
                        action = part.split()[1]
                        code = part.split()[2]
                    else:
                        action = "unknown"
                        code = "unknown"
                        excp = InterpreterError(f'No use case encountered')
                        self._handle_exception(excp)
                    action_data = {"command" : action}
                    add_influx_object(action_data,code=code)
            else:
                assert len(parts) == 3
                action = parts[2][2:]
                action_data = _parse_action(action)
                if action_data == {}:
                    exit()
                date, code, domain = None, None, None
                match = re.match(date_code_domain_pattern, parts[0])
                if match:
                    date = match.group("date")
                    code = match.group("code")
                    domain = match.group("domain")
                    add_influx_object(action_data,date,code,domain)
                else:
                    excp = InterpreterError(f'No use case encountered')
                    self._handle_exception(excp)
        
        return influx_objects

    def simulate(self, read_file, write_file, wait):
        with open(read_file,"r") as file:
            data = file.readlines()
        
        for line in data:
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
        "labware": None,
        "tip_diameter": None,
        "working_volume": None,
        "current_position": None,
        "target_position": None,
        "axes": None,
        "sequence": None,
        "window_status": None,
        "robot_name": None,
        "api_version": None,
        "mounts": None,
    }

    try:
        if "RETURN_TIP" in action:
            # Handle RETURN_TIP commands
            result["command"] = "RETURN_TIP"

        elif "Updating the window switch status" in action:
            # Handle window switch updates
            match = re.search(r"status:\s*(\w+)", action)
            if match:
                result["command"] = "update_window_status"
                result["window_status"] = match.group(1)
            else:
                result["details"] = action

        elif "Doing full configuration" in action or "Skipping configuration" in action:
            # Handle configuration messages
            match = re.search(r"(Doing|Skipping) full configuration on (\w+)", action)
            if match:
                result["command"] = f"{match.group(1).lower()}_configuration"
                result["details"] = f"Mount: {match.group(2)}"
            else:
                result["details"] = action

        elif "Instruments found:" in action:
            # Extract instruments and mounts
            match = re.search(r"Instruments found: \{(.+)\}", action)
            if match:
                result["command"] = "instruments_found"
                result["mounts"] = match.group(1)
            else:
                result["details"] = action

        elif "Connecting to motor controller" in action:
            # Handle motor controller connection logs
            result["command"] = "connect_motor_controller"
            result["details"] = action

        elif "Registering Central Routing Board" in action:
            # Handle routing board registration
            result["command"] = "register_routing_board"

        elif "Configuring GPIOs" in action:
            # Handle GPIO configuration logs
            result["command"] = "configure_gpios"
            result["details"] = action

        elif "Failed to detect central routing board revision" in action:
            # Handle routing board failures
            result["command"] = "routing_board_failure"
            result["details"] = action

        elif "loaded:" in action:
            # Handle pipette loading
            match = re.match(r"loaded: (\S+), pipette offset: \((.*?)\)", action)
            if match:
                result["command"] = "load_pipette"
                result["instrument"] = match.group(1)
                result["details"] = f"offset: {match.group(2)}"
            else:
                result["details"] = action

        elif "Updating tip rack diameter" in action:
            # Extract tip rack diameter
            match = re.search(r"tip diameter: ([\d\.]+) mm", action)
            if match:
                result["command"] = "update_tip_diameter"
                result["tip_diameter"] = match.group(1)
            else:
                result["details"] = action

        elif "Updating working volume" in action:
            # Extract working volume
            match = re.search(r"tip volume: (\d+) ul", action)
            if match:
                result["command"] = "update_working_volume"
                result["working_volume"] = match.group(1)
            else:
                result["details"] = action

        elif "Homing axes" in action:
            # Handle homing axes cases
            match = re.match(r"Homing axes (\w+) in sequence (\[.*\])", action)
            if match:
                result["command"] = "home_axes"
                result["axes"] = match.group(1)
                result["sequence"] = match.group(2)
            else:
                result["details"] = action.strip()

        elif "from position" in action:
            # Handle axis movements
            parts = action.split("from position")
            current_position = parts[0].strip().removeprefix("No axes move in").strip()
            target_position = parts[1].strip()

            result["command"] = "axis_move"
            result["current_position"] = current_position
            result["target_position"] = target_position

        elif "API server version" in action:
            # Extract API server version
            match = re.search(r"API server version:\s*(.*)", action)
            if match:
                result["command"] = "api_version"
                result["api_version"] = match.group(1)
            else:
                result["details"] = action

        elif "Robot Name" in action:
            # Extract robot name
            match = re.search(r"Robot Name:\s*(.*)", action)
            if match:
                result["command"] = "robot_name"
                result["robot_name"] = match.group(1)
            else:
                result["details"] = action

        elif "OT2CEP" in action and "open" in action:
            # Handle unstructured entries like "OT2CEP20210326B21 open"
            result["command"] = "log_entry"
            result["details"] = action

        elif action.startswith("command."):
            # Handle `command.` prefixed actions
            command, action_data = action.split(":", 1)
            result["command"] = command.removeprefix("command.")
            result = _extract_fields(action_data.strip(), result)

        else:
            # Fallback for unstructured or unexpected lines
            result["details"] = action.strip()

    except Exception as e:
        # Log the error in `details` and continue
        result["details"] = f"Parsing error: {str(e)} | Raw input: {action.strip()}"

    # Post-process `details` to remove extraneous values
    if result.get("details"):
        # Remove if it only contains punctuation or non-alphanumeric characters
        if re.fullmatch(r"\W*", result["details"]):
            result["details"] = None
    return result









def _extract_fields(action_data: str, result: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """
    Extract structured fields like instrument, location, volume, etc., from the action data.

    Args:
        action_data (str): The string containing structured action data.
        result (dict): The result dictionary to populate.

    Returns:
        dict: Updated result dictionary with extracted fields.
    """
    remaining_data = action_data

    # Parse instrument
    if "instrument:" in action_data:
        instrument_part = re.search(r"instrument:\s*([^,]+)", action_data)
        if instrument_part:
            result["instrument"] = instrument_part.group(1).strip()
            remaining_data = remaining_data.replace(instrument_part.group(0), "").strip()

    # Parse location (handle nested Point structures and labware descriptions)
    if "location:" in action_data:
        location_part = re.search(r"location:\s*(Location\(.*?\))", action_data)
        if location_part:
            result["location"] = location_part.group(1).strip()
            remaining_data = remaining_data.replace(location_part.group(0), "").strip()
        else:
            # Fallback: attempt to parse labware location if not nested
            simple_location_part = re.search(r"location:\s*([^,]+)", action_data)
            if simple_location_part:
                result["location"] = simple_location_part.group(1).strip()
                remaining_data = remaining_data.replace(simple_location_part.group(0), "").strip()

    # Extract labware if available (e.g., "labware=...")
    labware_part = re.search(r"labware=([^,]+)", action_data)
    if labware_part:
        result["labware"] = labware_part.group(1).strip()
        remaining_data = remaining_data.replace(labware_part.group(0), "").strip()

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

    # Clean up remaining data as details
    remaining_data = remaining_data.strip()

    # Assign remaining data to details only if it's meaningful
    if remaining_data and remaining_data not in [",", ""]:
        result["details"] = remaining_data
    else:
        result["details"] = None

    return result




