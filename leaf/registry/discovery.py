import os
import json
import importlib.util
from importlib.metadata import entry_points
from typing import Optional, Type, Any

from leaf.error_handler.exceptions import AdapterBuildError
from leaf.registry.loader import load_class_from_file
from leaf.registry.utils import ADAPTER_ID_KEY
from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.modules.output_modules.output_module import OutputModule
from leaf.modules.input_modules.external_event_watcher import ExternalEventWatcher

# Base directories for discovery
root_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
adapter_dir = os.path.join(root_dir, "adapters")
core_adapter_dir = os.path.join(adapter_dir, "core_adapters")
functional_adapter_dir = os.path.join(adapter_dir, "functional_adapters")
default_equipment_locations = [core_adapter_dir, functional_adapter_dir]

output_module_dir = os.path.join(root_dir, "modules", "output_modules")
input_module_dir = os.path.join(root_dir, "modules", "input_modules")


def discover_entry_point_equipment(
    group: str = "leaf.adapters",
) -> list[tuple[str, Type[Any]]]:
    """
    Discover equipment adapters exposed via setuptools entry points.

    Args:
        group: The entry point group name to search.

    Returns:
        A list of (adapter_id, class) tuples for discovered adapters.
    """
    discovered: list[tuple[str, Type[Any]]] = []

    for entry_point in entry_points(group=group):
        try:
            cls = entry_point.load()
            module_path = entry_point.module
            spec = importlib.util.find_spec(module_path)
            if not spec or not spec.origin:
                continue

            module_dir = os.path.dirname(spec.origin)
            device_json_path = os.path.join(module_dir, "device.json")

            if not os.path.exists(device_json_path):
                raise AdapterBuildError(
                    f"device.json not found for adapter at {module_dir}"
                )

            with open(device_json_path, "r") as f:
                device_info = json.load(f)
                adapter_id = device_info.get("adapter_id")
                if not adapter_id:
                    raise AdapterBuildError(
                        f"'adapter_id' not found in {device_json_path}"
                    )

                discovered.append((adapter_id, cls))

        except Exception:
            # Skip problematic entry points silently
            continue

    return discovered


def discover_local_equipment(
    base_dirs: Optional[list[str]] = None,
) -> list[tuple[str, Type[EquipmentAdapter]]]:
    """
    Discover equipment adapters in local directories containing `adapter.py` and `device.json`.

    Args:
        base_dirs: Optional list of directories to scan in addition to the defaults.

    Returns:
        A list of (adapter_id, class) tuples for discovered adapters.
    """
    discovered: list[tuple[str, Type[EquipmentAdapter]]] = []
    search_dirs = list(set((base_dirs or []) + default_equipment_locations))

    for base in search_dirs:
        if not os.path.exists(base):
            continue

        for root, _, files in os.walk(base):
            if "device.json" in files and "adapter.py" in files:
                json_fp = os.path.join(root, "device.json")
                py_fp = os.path.join(root, "adapter.py")

                try:
                    with open(json_fp, "r") as f:
                        data = json.load(f)
                        code = data.get(ADAPTER_ID_KEY)

                    if code:
                        cls = load_class_from_file(py_fp, base_class=EquipmentAdapter)
                        discovered.append((code, cls))
                except Exception:
                    continue

    return discovered


def discover_output_modules() -> list[tuple[str, Type[OutputModule]]]:
    """
    Discover output modules in the output module directory.

    Returns:
        A list of (class_name, class) tuples for discovered output modules.
    """
    discovered: list[tuple[str, Type[OutputModule]]] = []

    if not os.path.exists(output_module_dir):
        return discovered

    for file in os.listdir(output_module_dir):
        if file.endswith(".py"):
            path = os.path.join(output_module_dir, file)
            try:
                cls = load_class_from_file(path, base_class=OutputModule)
                discovered.append((cls.__name__, cls))
            except Exception:
                continue

    return discovered


def discover_external_inputs() -> list[tuple[str, Type[ExternalEventWatcher]]]:
    """
    Discover external input modules in the input module directory.

    Returns:
        A list of (class_name, class) tuples for discovered external input modules.
    """
    discovered: list[tuple[str, Type[ExternalEventWatcher]]] = []

    if not os.path.exists(input_module_dir):
        return discovered

    for file in os.listdir(input_module_dir):
        if file.endswith(".py"):
            path = os.path.join(input_module_dir, file)
            try:
                cls = load_class_from_file(path, base_class=ExternalEventWatcher)
                discovered.append((cls.__name__, cls))
            except Exception:
                continue

    return discovered
