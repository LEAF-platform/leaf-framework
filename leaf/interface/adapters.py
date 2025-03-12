import importlib
import os

import requests
from nicegui import ui

from leaf import register
from leaf.adapters.equipment_adapter import EquipmentAdapter


async def create_adapters_panel(tabs, adapters_tab, self) -> None:
    # Adapters tab
    ui.label('Installed adapters').classes('text-xl font-bold')
    # Obtain installed adapters
    adapters: dict[str, EquipmentAdapter] = register.load_adapters()
    if not adapters:
        ui.label('No adapters found')
        return
    # Display the adapters each as a button
    code_mirrors = {}
    with ui.row().style('width: 100%'):
        code_mirrors['code_block'] = ui.codemirror(value="# Click an adapter to view its example.yaml file", language='YAML').style('width: 45%')
        code_mirrors['placeholder'] = ui.codemirror(value="# Editor to create a new configuration...", language='YAML').style('width: 45%')
    for name, adapter in adapters.items():
        async def obtain_example(name: str=name, adapter: EquipmentAdapter=adapter) -> None:
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
                            code_mirrors["code_block"].value = f.read()
            except Exception as e:
                ui.notify(f'Error reading file: {e}', color='negative')

        ui.button(name, on_click=obtain_example)

    async def install_adapter() -> None:
        # Obtain adapters from marketplace
        url = "https://gitlab.com/LabEquipmentAdapterFramework/leaf-marketplace/-/raw/main/adapters.json?ref_type=heads"

        # Obtain the adapters from the marketplace
        response = requests.get(url)
        print(response)
        # Ensure the request was successful before parsing JSON
        if response.status_code == 200:
            adapters = response.json()
            print(adapters)  # Debugging output
            ui.notify(f'Available adapters: {len(adapters)}', color='positive')
            # Display the adapters
            async def pip_install() -> None:
                ui.notify(f'NOT YET!!!... Installing adapters: {adapters}', color='neutral')
            with ui.dialog() as dialog, ui.card():
                with ui.card():
                    ui.label('Available adapters').classes('text-xl font-bold')
                    for adapter in adapters:
                        ui.button(adapter['name'], on_click=pip_install)
                dialog.open()
        else:
            ui.notify(f"Error: Unable to fetch data (Status Code: {response.status_code})", color='negative')
    ui.button("Install new adapter", on_click=install_adapter, color='green')