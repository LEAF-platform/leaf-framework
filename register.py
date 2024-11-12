"""
This module provides utilities for loading equipment 
and output adapters dynamically from their respective
directories based on given codes. It dynamically 
imports Python modules from file paths,
and JSON files are used to map equipment 
adapters to the respective Python class that implements them.
"""

import importlib
import json
import importlib.util
import os
import logging

from core.modules.logger_modules.logger_utils import get_logger
from core.error_handler.exceptions import AdapterBuildError

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

root_dir = os.path.dirname(os.path.realpath(__file__))
adapter_dir = os.path.join(root_dir,"core", "adapters")
equipment_adapter_dirs = [
    os.path.join(adapter_dir, "core_adapters"),
    os.path.join(adapter_dir, "functional_adapters"),
]

equipment_key = "equipment_id"
output_adapter_dir = os.path.join(root_dir,"core", 
                                  "modules", "output_modules")


def get_equipment_adapter(code: str):
    """
    Searches for and returns the equipment adapter class corresponding to the given equipment code.

    This function traverses through the directories defined in `equipment_adapter_dirs` to locate a JSON file
    containing the specified equipment ID. It then attempts to load the corresponding Python class from the
    associated `.py` file.

    Args:
        code (str): The equipment code to search for.

    Returns:
        type: The class of the equipment adapter corresponding to the given code.

    Raises:
        FileNotFoundError: If the Python file associated with the equipment is not found.
        ValueError: If the equipment code is not found in the directories.
        AttributeError: If the class specified in the JSON file is not found in the Python module.
    """
    for adapter_dir in equipment_adapter_dirs:
        if os.path.exists(adapter_dir):
            for root, dirs, files in os.walk(adapter_dir):
                for file in files:
                    if file.endswith(".json"):
                        json_fp = os.path.join(root, file)
                        with open(json_fp, "r") as f:
                            data = json.load(f)
                            if equipment_key in data and data[equipment_key] == code:
                                python_fn = file.replace(".json", ".py")
                                python_fp = os.path.join(root, python_fn)
                                if os.path.exists(python_fp):
                                    return _load_class_from_file(
                                        python_fp, data["class"]
                                    )
                                else:
                                    raise AdapterBuildError(f"'{python_fp}' not found.")
                            else:
                                continue
    raise AdapterBuildError(f"{code} Unknown.")


def get_output_adapter(code: str):
    """
    Searches for and returns the output adapter class 
    corresponding to the given output adapter code.

    This function traverses the `output_adapter_dir` 
    to locate the appropriate Python file and attempts to
    dynamically load and return the class associated with the 
    given code.

    Args:
        code (str): The output adapter code to search for.

    Returns:
        type: The class of the output adapter corresponding 
        to the given code.

    Raises:
        FileNotFoundError: If the Python file for the output 
                           adapter is not found.
        AttributeError: If the class corresponding to the code is not 
                        found in the Python module.
    """
    for file in os.listdir(output_adapter_dir):
        if file.endswith(".py"):
            python_fp = os.path.join(output_adapter_dir, file)
            try:
                adapter_class = _load_class_from_file(python_fp, code)
                return adapter_class
            except (AdapterBuildError):
                continue
    else:
        raise AdapterBuildError(f"Class '{code}' not found.")


def _load_class_from_file(file_path: str, class_name: str):
    """
    Loads and returns a class dynamically from the given Python file.

    This function uses the `importlib` module to load the Python module
    from the given file path and returns the class with the 
    specified name if it exists.

    Args:
        file_path (str): The file path of the Python module.
        class_name (str): The name of the class to be 
                          loaded from the module.

    Returns:
        type: The class object from the specified 
              file and class name.

    Raises:
        AttributeError: If the class is not found 
                        in the specified module.
    """
    spec = importlib.util.spec_from_file_location(class_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, class_name):
        return getattr(module, class_name)
    else:
        raise AdapterBuildError(f"Class '{class_name}' not found in '{file_path}'.")
