import functools
import importlib
import os
import subprocess
import sys

import requests
from nicegui import ui

from leaf import register
from leaf.adapters.equipment_adapter import EquipmentAdapter

installed_adapters: dict[str, EquipmentAdapter] = register.load_adapters()

async def create_adapters_panel(tabs, adapters_tab, self) -> None:
    # Adapters tab
    ui.label('Installed adapters').classes('text-xl font-bold')
    # Obtain installed adapters

    if not installed_adapters:
        ui.label('No adapters found')
    # Display the adapters each as a button
    code_mirrors = {}
    with ui.row().style('width: 100%'):
        code_mirrors['code_block'] = ui.codemirror(value="# Click an adapter to view its example.yaml file", language='YAML').style('width: 45%')
        code_mirrors['placeholder'] = ui.codemirror(value="# Editor to create a new configuration...", language='YAML').style('width: 45%')
    with ui.row().style('width: 100%'):
        for name, installed_adapter in installed_adapters.items():
            async def obtain_example(name: str=name, adapter: EquipmentAdapter=installed_adapter) -> None:
                ui.notify(f'Selected adapter: {name}', color='positive')
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
                        else:
                            ui.notify(f'No example.yaml file found at {file_path}', color='error')
                            code_mirrors["code_block"].value = "# No example.yaml file found at {file_path}"
                except Exception as e:
                    ui.notify(f'Error reading file: {e}', color='negative')

            if installed_adapter:
                ui.button(name, on_click=obtain_example)
            else:
                ui.button(name, on_click=obtain_example).disable()

    async def install_adapter() -> None:
        # Obtain adapters from marketplace
        url = "https://gitlab.com/LabEquipmentAdapterFramework/leaf-marketplace/-/raw/main/adapters.json?ref_type=heads"

        # Obtain the adapters from the marketplace
        response = requests.get(url)
        # Ensure the request was successful before parsing JSON
        if response.status_code == 200:
            public_adapters = response.json()
            ui.notify(f'Available adapters: {len(public_adapters)}', color='positive')
            # Display the adapters
            async def pip_install(dialog: ui.dialog, adapter: dict[str, str]) -> None:
                dialog.close()
                ui.notify(f'Installing adapter: {adapter}', color='positive')
                # Perform a pip install on a git url
                if adapter['repository'].startswith('https://'):
                    repository = "git+" + adapter['repository']
                elif adapter['repository'].startswith('git://'):
                    repository = adapter['repository']
                else:
                    repository = None
                    ui.notify(f"Invalid repository URL: {adapter['repository']}", color='negative')
                # Using the internal pip module
                if repository:
                    # Identify location of pip
                    command = [sys.executable, '-m','pip', 'install', repository]
                    subprocess.check_call(command)
                    ui.notify(f'Installed adapter: {adapter["name"]}', color='positive')
                    global installed_adapters
                    installed_adapters = register.load_adapters()
            with ui.dialog() as dialog, ui.card():
                with ui.card():
                    ui.label('Available adapters').classes('text-xl font-bold')
                    for public_adapter in public_adapters:
                        # Currently no way to check if an adapter is already installed with the information provided
                        ui.button(public_adapter['name'], on_click=functools.partial(pip_install, dialog, public_adapter))
                dialog.open()
        else:
            ui.notify(f"Error: Unable to fetch data (Status Code: {response.status_code})", color='negative')
    # Install new adapter button only when not already installed
    # if installed_adapter['name']
    with ui.row().style('width: 100%'):
        ui.button("Install new adapter", on_click=install_adapter, color='green')
    with ui.row().style('width: 100%'):
        # Write text to explain disabled adapter buttons
        ui.label("Disabled adapters did not install successfully. Check logs for more details.").style('color: grey')