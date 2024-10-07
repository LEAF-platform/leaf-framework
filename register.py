import importlib
import json
import importlib.util
import os

adapter_dir = os.path.join("core","adapters")
equipment_adapter_dirs = [os.path.join(adapter_dir,"core_adapters"),
                       os.path.join(adapter_dir,"functional_adapters")]

equipment_key = 'equipment_id'
output_adapter_dir = os.path.join("core","modules","output_modules")

def get_equipment_adapter(code):
    for adapter_dir in equipment_adapter_dirs:
        if os.path.exists(adapter_dir):
            for root, dirs, files in os.walk(adapter_dir):
                for file in files:
                    if file.endswith(".json"):
                        json_fp = os.path.join(root, file)
                        with open(json_fp, 'r') as f:
                            data = json.load(f)
                            if equipment_key in data and data[equipment_key] == code:
                                python_fn = file.replace('.json', '.py')
                                python_fp = os.path.join(root, python_fn)
                                if os.path.exists(python_fp):
                                    return _load_class_from_file(python_fp, data["class"])
                                else:
                                    raise FileNotFoundError(f"'{python_fp}' not found.")
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

