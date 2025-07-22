import logging
import os
import subprocess
import sys
import time
from functools import partial
from pathlib import Path
from typing import Any

import httpx

from leaf.registry.discovery import get_all_adapter_codes
from nicegui import ui, app

logger = logging.getLogger()

# Define adapter_content outside to maintain scope
adapter_content = ui.column()

class LogElementHandler(logging.Handler):
    """A logging handler that emits messages to a log element."""

    def __init__(self, element: ui.log, level: int = logging.NOTSET) -> None:
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.element.push(msg)
        except Exception:
            self.handleError(record)


# def load_content() -> None:
#     url = "https://gitlab.com/LabEquipmentAdapterFramework/leaf-marketplace/-/raw/main/adapter_cache.json"
#     response = httpx.get(url)
#     adapter_content.clear()  # Clear previous content
#     data = response.json()
#     # Get the list of installed adapters to filter out
#     installed_adapters = get_all_adapter_codes()
#     with ui.row().classes('w-full'):
#         for index, adapter in enumerate(data):
#             if index % 4 == 0 and index != 0:
#                 # Create a new row every 4 adapters
#                 ui.row().classes('w-full')
#             with adapter_content:
#                 with ui.card().classes('max-w-lg'):
#                     # Top right corner for the installation button
#                     ui.button("Install", on_click=lambda a=adapter: install_adapter(a)).classes('absolute top-2 right-2 bg-blue-500 text-white font-bold py-1 px-2 rounded')
#                     ui.label(f"Adapter: {adapter['name']}")
#                     # ui.label(f"Description: {adapter['description']}")


def install_adapter(adapter: dict[Any, Any]) -> None:
    print(f"Installing {adapter}...")
    repository = adapter['repo_url']

    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", f'git+{repository}'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"Failed to install {adapter['name']}: {result.stderr}")
        ui.notify(f"Failed to install {adapter['name']}", color='red')
        return

    logger.info(f"Installed {adapter['name']}:\n{result.stdout}")
    ui.notify(f"Installed {adapter['name']}")
    time.sleep(1)

    logger.info("Restarting...")
    os.execl(sys.executable, sys.executable, *sys.argv)

def uninstall_adapter(installed_adapter: dict) -> None:
    print(f"Uninstalling {installed_adapter}...")
    # repository = adapter['repo_url']
    package_name = installed_adapter.get('name')  # or parse from repo_url if missing

    if not package_name:
        print(f"Cannot uninstall adapter without package name: {installed_adapter}")
        return

    logger.info(f"Uninstalling {package_name}...")
    ui.notify(f"Uninstalling {package_name}", color='red')

    result = subprocess.run(
        [sys.executable, "-m", "pip", "uninstall", "-y", package_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode == 0:
        print(f"Uninstalled {package_name} successfully.")
        # Restart the application to apply changes
        logger.info(f"Uninstalled {package_name} successfully.")
        ui.notify(f"Uninstalled {package_name}")
        time.sleep(1)
        logger.info("Restarting...")
        os.execl(sys.executable, sys.executable, *sys.argv)
    else:
        print(f"Failed to uninstall {package_name}.\nError: {result.stderr}")



def start_nicegui(port: int = 8080) -> None:
    ui.page('/')
    
      # Add favicon
    ui.add_head_html('<link rel="icon" type="image/x-icon" href="https://nicegui.io/favicon.ico">')

    # Dark mode toggle
    dark = ui.dark_mode()

    # Header layout
    with ui.header().style('background-color: rgb(133, 171, 215); color: white; padding: 10px;'):
        with ui.row().classes('justify-between items-center w-full'):
            ui.label('LEAF Monitoring System').classes('text-2xl font-bold')
            # Add a sun icon for light mode toggle
            is_dark = {'state': False}

            def toggle_mode():
                if is_dark['state']:
                    dark.disable()
                    button.icon = 'light_mode'  # sun icon
                else:
                    dark.enable()
                    button.icon = 'dark_mode'  # moon icon
                is_dark['state'] = not is_dark['state']

            button = ui.button('', icon='light_mode', on_click=toggle_mode).props('flat round').classes('text-white bg-transparent hover:bg-blue-700 focus:bg-blue-700')

    # Tabs
    with ui.tabs().classes('w-full') as tabs:
        config_tab: ui.tab = ui.tab('Configuration')
        logs_tab = ui.tab('Logs')
        docs_tab = ui.tab('Documentation')
        adapters_tab = ui.tab('Adapters')

    with ui.tab_panels(tabs, value=logs_tab).classes('w-full'):
        # Configuration tab
        with ui.tab_panel(config_tab).classes('w-full'):
            ui.label('LEAF Configuration').classes('text-xl font-bold')
            # Code editor for YAML
            from leaf.start import context
            import os
            curr_dir: Path = Path(os.path.dirname(os.path.realpath(__file__)))
            configuration_path: Path = curr_dir / ".." / 'config' / 'configuration.yaml'
            if os.path.exists(configuration_path):
                with open(configuration_path, 'r') as file:
                        context.config_yaml = file.read()
            else:
                logger.error("Configuration file not found")
                context.config_yaml = '''# No configuration file found'''

            with ui.row().classes('w-full'):
                # Change width to 50% and height to 500px
                editor = ui.codemirror(value=context.config_yaml, language="YAML", theme='basicDark').classes('w-1/2 h-96')

                # Markdown next to the editor
                ui.markdown('''
                        # Configuration File
                        The configuration file is a YAML file that defines the equipment instances and their requirements.
                        The file is loaded when the application starts and can be modified in this editor.
                        ''').classes('w-1/4 h-96')

            # Button to start/restart the adapters
            def restart_app(restart: bool) -> None:
                # Write new configuration to file
                if restart:
                    logger.debug("Writing new configuration to file... " + str(configuration_path) + " with content: " + editor.value)
                    with open(configuration_path, 'w') as file:
                        file.write(editor.value)
                    logger.info("Restarting...")
                    os.execl(sys.executable, sys.executable, *sys.argv)
                else:
                    # Close the current window and shutdown the app
                    ui.run_javascript('window.open(location.href, "_self", "");window.close()')
                    os.execl(sys.executable, sys.executable, sys.argv[0], "--shutdown")


            with ui.row().classes('w-full'):
                # Button to restart the app and sent the editor.value to the restart_app function
                ui.button('Restart App', on_click=partial(restart_app, True))
                ui.button('Stop App', on_click=partial(restart_app, False))



        # Logs tab
        with ui.tab_panel(logs_tab):
            ui.label('LEAF Logs').classes('text-xl font-bold')
            log = ui.log(max_lines=1000).classes('w-full h-96 overflow-y-auto')
            handler = LogElementHandler(log)
            logger.addHandler(handler)
            ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))
            logger.info("bla?")

        # Plugins tab
        with ui.tab_panel(adapters_tab):
            # Create a two rows layout

            # Top row installed adapters
            with ui.row().classes('w-full'):
                ui.label('Installed Adapters:').classes('text-xl font-bold')
            with ui.row().classes('w-full'):
                # Get the list of installed adapters
                installed_adapters = get_all_adapter_codes()
                for installed_adapter in installed_adapters:
                    print(installed_adapter)
                    # Create a button for each adapter
                    with ui.card().tight().classes(
                            'w-[200px] h-[200px] flex flex-col justify-start p-4 shadow-md border border-gray-100 rounded-lg hover:shadow-lg hover:-translate-y-1 transition-all duration-200'):
                        ui.label(installed_adapter['code']).classes('text-xl font-bold text-center truncate w-full')
                        ui.label(installed_adapter['name']).classes('text-sm text-gray-500 text-center truncate w-full')
                        ui.element('div').classes('flex-grow')
                        ui.button('Uninstall', on_click=lambda _:uninstall_adapter(installed_adapter)).classes(
                            'text-xs bg-red-500 text-white font-semibold py-1 px-2 rounded w-full'
                        ).props('flat round')
                        # Bottom row for installing new adapters
            with ui.row().classes('w-full'):
                ui.label('Install New Adapters:').classes('text-xl font-bold')
            # Load content for available adapters
            # #load_content()
            url = "https://gitlab.com/LabEquipmentAdapterFramework/leaf-marketplace/-/raw/main/adapter_cache.json"
            response = httpx.get(url)
            adapter_content.clear()  # Clear previous content
            data = response.json()
            with ui.row().classes('w-full flex-wrap gap-4'):
                for adapter in data:
                    with ui.card().tight().classes(
                            'w-[200px] h-[200px] flex flex-col justify-start p-4 shadow-md border border-gray-100 rounded-lg '
                            'hover:shadow-lg hover:-translate-y-1 transition-all duration-200'
                    ):
                        ui.label(adapter['adapter_id']).classes('text-xl font-bold text-center truncate w-full')
                        ui.label(adapter.get('name', '')).classes('text-sm text-gray-500 text-center truncate w-full')
                        ui.element('div').classes('flex-grow')
                        ui.button("Install", on_click=lambda a=adapter: install_adapter(a)).classes(
                            'text-xs bg-blue-500 text-white font-semibold py-1 px-2 rounded w-full'
                        ).props('flat round')
            #             # ui.label(f"Description: {adapter['description']}")
            #
            # # Acquire a list of all available adapters
            # adapters = get_all_adapter_codes()
            # # Create a list of adapter names
            # ui.label("Available Adapters:").classes('text-xl font-bold')
            # for adapter in adapters:
            #     # Create a button for each adapter
            #     ui.label(adapter['code']).classes('text-xl font-bold')
            # # Create a dialog to install adapters
            # with ui.dialog() as install_adapters, ui.card():
            #     # Just a black X button in the top right corner
            #     ui.button('', icon='close', on_click=install_adapters.close).props('flat round').classes('absolute top-2 right-2')
            #     ui.label('') # Empty label to create space for the button
            #     # global adapter_content
            #     # adapter_content = ui.label('')
            #
            #
            # # Button to open the dialog
            # ui.button('Install Adapters', on_click=lambda: [load_content(), install_adapters.open()]).classes('bg-blue-500 text-white font-bold py-2 px-4 rounded')

        # Documentation tab
        with ui.tab_panel(docs_tab):
            ui.markdown('''
                    # LEAF System Documentation
    
                    LEAF (Laboratory Equipment Adapter Framework) is a system for monitoring laboratory equipment and sending data to the cloud.
    
                    ## Quick Start
    
                    1. Load a configuration file in the Configuration tab
                    2. Start the adapters using the "Start/Restart Adapters" button in the Dashboard tab
                    3. Monitor your equipment and system status in the Dashboard
    
                    ## Configuration
    
                    The configuration file follows a YAML format with these main sections:
    
                    - `OUTPUTS`: Defines where data should be sent
                    - `EQUIPMENT_INSTANCES`: Defines the laboratory equipment to monitor
    
                    For more detailed documentation, visit [leaf.systemsbiology.nl](https://leaf.systemsbiology.nl)
                    ''')

    ui.run(reload=False, port=port)