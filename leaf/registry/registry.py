from typing import Type, Any, Literal, Dict

from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.modules.output_modules.output_module import OutputModule
from leaf.modules.input_modules.external_event_watcher import ExternalEventWatcher
from leaf.error_handler.exceptions import AdapterBuildError

PluginType = Literal["equipment", "output", "external_input"]

# Internal registry map
_registry: Dict[PluginType, Dict[str, Type[Any]]] = {
    "equipment": {},
    "output": {},
    "external_input": {},
}


def register(plugin_type: PluginType, code: str, cls: Type[Any]) -> None:
    """
    Register a plugin class under a plugin type and code.
    """
    _registry[plugin_type][code] = cls


def get(plugin_type: PluginType, code: str) -> Type[Any]:
    """
    Retrieve a class from the registry by plugin type and code.

    Raises:
        AdapterBuildError if the plugin is not found.
    """
    try:
        return _registry[plugin_type][code]
    except KeyError:
        raise AdapterBuildError(f"No {plugin_type} class registered for code '{code}'")


def get_equipment_adapter(code: str) -> Type[EquipmentAdapter]:
    """
    Retrieve an EquipmentAdapter class by code.
    """
    cls = get("equipment", code)
    if not issubclass(cls, EquipmentAdapter):
        raise AdapterBuildError(f"'{code}' is not an EquipmentAdapter")
    return cls


def get_output_adapter(code: str) -> Type[OutputModule]:
    """
    Retrieve an OutputModule class by code.
    """
    cls = get("output", code)
    if not issubclass(cls, OutputModule):
        raise AdapterBuildError(f"'{code}' is not an OutputModule")
    return cls


def get_external_input(code: str) -> Type[ExternalEventWatcher]:
    """
    Retrieve an ExternalEventWatcher class by code.
    """
    cls = get("external_input", code)
    if not issubclass(cls, ExternalEventWatcher):
        raise AdapterBuildError(f"'{code}' is not an ExternalEventWatcher")
    return cls


def all_registered(plugin_type: PluginType) -> Dict[str, Type[EquipmentAdapter]]:
    """
    Return all registered classes of a given plugin type.
    """
    return dict(_registry[plugin_type])


def discover_from_config(config: dict[str, Any], external_path: str = None) -> None:
    """
    Discover and register only the plugins referenced in the given configuration.

    Args:
        config: Parsed configuration dictionary.
        external_path: Optional external directory to include in discovery.
    """
    from leaf.registry import discovery

    # Collect plugin codes from the config
    equipment_codes = {
        instance["equipment"]["adapter"]
        for instance in config.get("EQUIPMENT_INSTANCES", [])
    }
    output_codes = {output["plugin"] for output in config.get("OUTPUTS", [])}
    external_input_codes = {
        instance["equipment"]["external_input"]["plugin"]
        for instance in config.get("EQUIPMENT_INSTANCES", [])
        if "external_input" in instance["equipment"]
    }

    # Discover and register equipment adapters
    discovered_equipment = (
        discovery.discover_entry_point_equipment()
        + discovery.discover_local_equipment([external_path] if external_path else [])
    )
    for code, cls in discovered_equipment:
        if code in equipment_codes:
            register("equipment", code, cls)

    # Discover and register output modules
    for code, cls in discovery.discover_output_modules():
        if code in output_codes:
            register("output", code, cls)

    # Discover and register external input modules
    for code, cls in discovery.discover_external_inputs():
        if code in external_input_codes:
            register("external_input", code, cls)
