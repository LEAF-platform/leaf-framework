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

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

adapter_dir = os.path.join("core","adapters")
equipment_adapter_dirs = [os.path.join(adapter_dir,"core_adapters"),
                          os.path.join(adapter_dir,"functional_adapters")]

equipment_key = 'equipment_id'
output_adapter_dir = os.path.join("core","modules","output_modules")

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
    logger.debug(f"Checking 'code': {code}")
    for adapter_dir in equipment_adapter_dirs:
        logger.debug(f"Checking {adapter_dir}")
        if os.path.exists(adapter_dir):
            logger.debug(f"Found {adapter_dir}")
            for root, dirs, files in os.walk(adapter_dir):
                for file in files:
                    if file.endswith(".json"):
                        logger.debug(f"Checking {file}")
                        json_fp = os.path.join(root, file)
                        with open(json_fp, 'r') as f:
                            data = json.load(f)
                            logger.debug(f"Checking {equipment_key} in {data}")
                            if equipment_key in data and data[equipment_key] == code:
                                python_fn = file.replace('.json', '.py')
                                logger.debug(f"Found {python_fn}")
                                python_fp = os.path.join(root, python_fn)
                                logger.debug(f"Checking {python_fp}")
                                if os.path.exists(python_fp):
                                    return _load_class_from_file(python_fp, data["class"])
                                else:
                                    raise FileNotFoundError(f"'{python_fp}' not found.")
                            else:
                                logger.debug("Check check check...")
                                try:
                                    logger.debug(f"Is {equipment_key} in {data}? {equipment_key in data} and {data[equipment_key]} == {code}? {data[equipment_key] == code}")
                                except:
                                    pass
                    else:
                        logger.debug(f"{file} not a json file.")
        else:
            logger.debug(f"{adapter_dir} not found.")
    raise ValueError(f'{code} Unknown.')

            

def get_output_adapter(code: str):
    """
    Searches for and returns the output adapter class corresponding to the given output adapter code.

    This function traverses the `output_adapter_dir` to locate the appropriate Python file and attempts to 
    dynamically load and return the class associated with the given code.

    Args:
        code (str): The output adapter code to search for.

    Returns:
        type: The class of the output adapter corresponding to the given code.

    Raises:
        FileNotFoundError: If the Python file for the output adapter is not found.
        AttributeError: If the class corresponding to the code is not found in the Python module.
    """
    for file in os.listdir(output_adapter_dir):
        if file.endswith(".py"):
            python_fp = os.path.join(output_adapter_dir, file)
            try:
                adapter_class = _load_class_from_file(python_fp, code)
                return adapter_class
            except (FileNotFoundError, AttributeError):
                continue


def _load_class_from_file(file_path: str, class_name: str):
    """
    Loads and returns a class dynamically from the given Python file.

    This function uses the `importlib` module to load the Python module from the given file path and returns 
    the class with the specified name if it exists.

    Args:
        file_path (str): The file path of the Python module.
        class_name (str): The name of the class to be loaded from the module.

    Returns:
        type: The class object from the specified file and class name.

    Raises:
        AttributeError: If the class is not found in the specified module.
    """
    spec = importlib.util.spec_from_file_location(class_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, class_name):
        return getattr(module, class_name)
    else:
        raise AttributeError(f"Class '{class_name}' not found in '{file_path}'.")
