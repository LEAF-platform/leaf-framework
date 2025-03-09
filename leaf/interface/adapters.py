import importlib
import os

from nicegui import ui

from leaf import register
from leaf.adapters.equipment_adapter import EquipmentAdapter


async def create_adapters_panel(tabs, config_tab, self) -> None:
    # Adapters tab
    ui.label('Adapters').classes('text-xl font-bold')
    # Obtain installed adapters
    adapters: dict[str, EquipmentAdapter] = register.load_adapters()
    if not adapters:
        ui.label('No adapters found')
        return
    # Display the adapters each as a button
    code_mirrors = {}
    for name, adapter in adapters.items():
        async def xxx(name: str=name, adapter: EquipmentAdapter=adapter) -> None:
            ui.notify(f'Selected adapter: {name} {adapter}', color='positive')
            # Read a file inside the adapter package
            try:
                # Get module location
                module_name = adapter.__module__
                spec = importlib.util.find_spec(module_name)
                if spec and spec.origin:
                    package_dir = os.path.dirname(spec.origin)  # Get directory of the adapter module
                    file_path = os.path.join(package_dir, "example.yaml")  # Correct way to join paths
                    if os.path.exists(file_path):
                        with open(file_path, "r") as f:
                            code_mirror = ui.codemirror(value=f.read(), language="YAML").style('width: 100%')
                            if module_name in code_mirrors:
                                code_mirrors[module_name].delete()
                            code_mirrors[module_name] = code_mirror
            except Exception as e:
                ui.notify(f'Error reading file: {e}', color='negative')

        ui.button(name, on_click=xxx)
