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

            

def get_output_adapter(code):
    for file in os.listdir(output_adapter_dir):
        if file.endswith(".py"):
            python_fp = os.path.join(output_adapter_dir, file)
            try:
                adapter_class = _load_class_from_file(python_fp, 
                                                        code)
                return adapter_class
            except (FileNotFoundError, AttributeError):
                continue


def _load_class_from_file(file_path, class_name):
    spec = importlib.util.spec_from_file_location(class_name, 
                                                  file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, class_name):
        return getattr(module, class_name)
    else:
        raise AttributeError(f"Class '{class_name}' not found in '{file_path}'.")

