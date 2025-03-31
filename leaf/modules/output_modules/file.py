import json
import os
import random
from typing import Any
from typing import Optional
from typing import Union
from leaf.modules.output_modules.output_module import OutputModule
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import SeverityLevel


class FILE(OutputModule):
    def __init__(self, filename: str, fallback: Optional[OutputModule] = None, 
                 error_holder: Optional[ErrorHolder] = None) -> None:
        super().__init__(fallback=fallback, error_holder=error_holder)
        self.filename = filename

    def _handle_file_error(self, error) -> None:
        """
        Handles file-related exceptions consistently 
        with a structured error message.
        """
        if isinstance(error, FileNotFoundError):
            message = f"File not found '{self.filename}'"
            severity = SeverityLevel.WARNING
        elif isinstance(error, PermissionError):
            message = f"Permission denied when accessing '{self.filename}'"
            severity = SeverityLevel.CRITICAL
        elif isinstance(error, OSError):
            message = f"I/O error in file '{self.filename}': {error}"
            severity = SeverityLevel.ERROR
        elif isinstance(error, json.JSONDecodeError):
            message = f"JSON decode error when reading '{self.filename}' "
            severity = SeverityLevel.WARNING
        else:
            message = f"Unexpected error in file '{self.filename}': {error}"
            severity = SeverityLevel.WARNING

        self._handle_exception(ClientUnreachableError(message,
                                                      output_module=self,
                                                      severity=severity))

    def transmit(self, topic: str, data: Optional[Union[str, dict]] = None) -> bool:
        """
        Transmit data to the file associated with a specific topic.
        """
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    try:
                        file_data = json.load(f)
                    except json.JSONDecodeError:
                        file_data = {}
            else:
                file_data = {}

            if topic in file_data:
                if not isinstance(file_data[topic], list):
                    file_data[topic] = [file_data[topic]]
            else:
                file_data[topic] = []

            if data is not None:
                file_data[topic].append(data)

            with open(self.filename, 'w') as f:
                json.dump(file_data, f, indent=4)
            return True

        except (OSError, IOError, json.JSONDecodeError) as e:
            self._handle_file_error(e)
            if self._fallback:
                return self._fallback.transmit(topic, data=data)
            return False

    def retrieve(self, topic: str) -> Any | None:
        """
        Retrieve data associated with a specific topic.
        """
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    try:
                        file_data = json.load(f)
                    except json.JSONDecodeError as e:
                        self._handle_file_error(e)
                        return None
            else:
                return None

            return file_data.get(topic, None)

        except (OSError, IOError) as e:
            self._handle_file_error(e)
            return None
    
    def pop(self, key: str = None) -> tuple[str, Any] | None:
        """
        Retrieve and remove a record from the file. 
        If a specific key is provided, retrieve and remove all values under that key.
        If no key is provided, retrieve and remove one element from a random key's list.
        The key is removed when the last element is taken.

        Args:
            key (Optional[str]): The key of the record to retrieve and remove. 
                                If None, a random record is retrieved and removed.

        Returns:
            Optional[tuple[str, Any]]: A tuple of the key and the retrieved value,
                                    or None if the key does not exist or the 
                                    file is empty.
        """
        try:
            if not os.path.exists(self.filename):
                return None

            with open(self.filename, 'r') as f:
                try:
                    file_data = json.load(f)
                except json.JSONDecodeError as e:
                    self._handle_file_error(e)
                    return None

            if key is not None:
                if key in file_data:
                    values = file_data.pop(key)
                    with open(self.filename, 'w') as f:
                        json.dump(file_data, f, indent=4)
                    return key, values
                else:
                    return None

            if not file_data:
                return None

            random_key = random.choice(list(file_data.keys()))
            values = file_data[random_key]

            if isinstance(values, list):
                popped_value = values.pop(0)
                if values:
                    file_data[random_key] = values
                else:
                    file_data.pop(random_key)
            else:
                popped_value = values
                file_data.pop(random_key)

            with open(self.filename, 'w') as f:
                json.dump(file_data, f, indent=4)
            return random_key, popped_value

        except (OSError, IOError, json.JSONDecodeError) as e:
            self._handle_file_error(e)
            return None
    
    def is_connected(self) -> bool:
        """
        Check if the FILE module is always connected.

        Returns:
            bool: Always returns True.
        """
        return True
    
    def connect(self) -> None:
        """
        Connect method for FILE module (no-op as files are always accessible).
        """
        pass
        
    def disconnect(self) -> None:
        """
        Disconnect method for FILE module (no-op as files are always accessible).
        """
        pass

