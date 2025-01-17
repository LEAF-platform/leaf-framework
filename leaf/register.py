"""
This module provides utilities for loading equipment 
and output adapters dynamically from their respective
directories based on given codes. It dynamically 
imports Python modules from file paths,
and JSON files are used to map equipment 
adapters to the respective Python class that implements them.
"""
import sys
import importlib
import json
import inspect
import importlib.util
import os
from shutil import copytree
import logging
from typing import List
from importlib.metadata import entry_points

from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.error_handler.exceptions import AdapterBuildError

root_dir = os.path.dirname(os.path.realpath(__file__))
adapter_dir = os.path.join(root_dir, "adapters")
core_adapter_dir = os.path.join(adapter_dir, "core_adapters")
functional_adapter_dir = os.path.join(adapter_dir, "functional_adapters")
equipment_adapter_dirs = [
    core_adapter_dir,
    functional_adapter_dir
]

equipment_key = "equipment_id"
output_adapter_dir = os.path.join(root_dir, "modules", "output_modules")

def load_adapters() -> dict[str, EquipmentAdapter]:
    """
    Loads and returns a dictionary mapping `equipment_id` to 
    the corresponding EquipmentAdapter class from the installed packages.
    This function looks for a `device.json` file in the same package directory
    as the adapter and uses the `equipment_id` defined in it for the mapping.

    Returns:
        dict[str, EquipmentAdapter]: A mapping of equipment IDs to adapter classes.

    Raises:
        AdapterBuildError: If the `device.json` file is missing or invalid.
    """
    adapters = {}
    for entry_point in entry_points(group="leaf.adapters"):
        try:
            adapter_class = entry_point.load()
            module_path = entry_point.module
            module_dir = os.path.dirname(importlib.util.find_spec(module_path).origin)

            device_json_path = os.path.join(module_dir, "device.json")
            if not os.path.exists(device_json_path):
                raise AdapterBuildError(f"device.json not found for adapter at {module_dir}")

            with open(device_json_path, "r") as f:
                device_info = json.load(f)
                if "equipment_id" not in device_info:
                    raise AdapterBuildError(f"'equipment_id' not found in {device_json_path}")
                equipment_id = device_info["equipment_id"]
                adapters[equipment_id] = adapter_class

        except Exception as e:
            logging.error(f"Failed to load adapter from entry point {entry_point.name}: {e}")

    return adapters


def get_equipment_adapter(code: str,external_adapter=None) -> EquipmentAdapter:
    """
    Searches for and returns the equipment adapter class corresponding to the given equipment code.

    This function traverses through the directories defined in `equipment_adapter_dirs` to locate a 
    `device.json` file containing the specified equipment ID. It then attempts to load the corresponding
    class from the `adapter.py` file in the same directory.

    Args:
        code (str): The equipment code to search for.

    Returns:
        type: The class of the equipment adapter corresponding to the given code.

    Raises:
        AdapterBuildError: If the adapter files or class cannot be found for the given code.
    """
    available_adapters: List[EquipmentAdapter] = load_adapters()
    if external_adapter:
        copy_path = os.path.join(functional_adapter_dir,
                                 os.path.basename(external_adapter))
        if not os.path.isdir(copy_path):
            copytree(external_adapter, copy_path)
        else:
            logging.warning(f'Given external path: {external_adapter} but adapter exists in adapter directory.')
        sys.path.append(copy_path)
    for adapter_dir in equipment_adapter_dirs:
        if code in available_adapters:
            return available_adapters[code]
        if os.path.exists(adapter_dir):
            for root, dirs, files in os.walk(adapter_dir):
                json_fp = os.path.join(root, "device.json")
                python_fp = os.path.join(root, "adapter.py")

                if os.path.exists(json_fp) and os.path.exists(python_fp):
                    with open(json_fp, "r") as f:
                        data = json.load(f)
                        if equipment_key in data and data[equipment_key] == code:
                            return _load_class_from_file(python_fp)

    raise AdapterBuildError(f"Adapter for code '{code}' not found.")


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


def _load_class_from_file(file_path: str, class_name: str = None):
    """
    Loads and returns a class dynamically from the given Python file.

    If a class name is provided, it attempts to load that specific class.
    If no class name is provided, it searches for the first class that
    inherits from EquipmentAdapter.

    Args:
        file_path (str): The file path of the Python module.
        class_name (str, optional): The name of the class to be loaded. If None,
                                    the function will search for a class that
                                    inherits from EquipmentAdapter.

    Returns:
        type: The class object from the specified file and class name, or the
              first class inheriting from EquipmentAdapter if no class name is provided.

    Raises:
        AdapterBuildError: If the specified class or a class inheriting from
                           EquipmentAdapter is not found.
    """
    spec = importlib.util.spec_from_file_location("module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if class_name:
        if hasattr(module, class_name):
            return getattr(module, class_name)
        else:
            raise AdapterBuildError(f"Class '{class_name}' not found in '{file_path}'.")
    else:
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, EquipmentAdapter) and obj is not EquipmentAdapter:
                return obj

        raise AdapterBuildError(f"No class inheriting from EquipmentAdapter found in '{file_path}'.")
