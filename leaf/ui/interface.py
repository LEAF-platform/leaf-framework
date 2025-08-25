import logging
import os
import subprocess
import sys
import time
from functools import partial
from pathlib import Path
from typing import Any, Dict

import httpx
from nicegui import ui

from leaf.registry.discovery import get_all_adapter_codes

logger = logging.getLogger()

# Constants
DEFAULT_PORT = 8080
MARKETPLACE_URL = "https://gitlab.com/LabEquipmentAdapterFramework/leaf-marketplace/-/raw/main/adapter_cache.json"
MAX_LOG_LINES = 1000
CONFIGURATION_FILENAME = 'configuration.yaml'

# UI Constants
CARD_WIDTH_CLASS = 'w-64'
HEADER_CLASSES = 'leaf-header'
CARD_CLASSES = 'leaf-card'
TAB_CLASSES = 'leaf-tab'

# Color constants
STATUS_COLORS = {
    'online': 'status-online',
    'offline': 'status-offline', 
    'warning': 'status-warning'
}

BUTTON_COLORS = {
    'primary': 'bg-blue-500 hover:bg-blue-600',
    'success': 'bg-green-500 hover:bg-green-600',
    'danger': 'bg-red-500 hover:bg-red-600',
    'warning': 'bg-orange-500 hover:bg-orange-600',
    'info': 'bg-gray-500 hover:bg-gray-600',
    'purple': 'bg-purple-500 hover:bg-purple-600'
}

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



def install_adapter(adapter: dict[Any, Any]) -> None:
    """
    Install a LEAF adapter from a Git repository.
    
    Args:
        adapter: Dictionary containing adapter metadata including 'repo_url' and 'name'
    """
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

def uninstall_adapter(installed_adapter: Dict) -> None:
    """
    Uninstall a LEAF adapter package.
    
    Args:
        installed_adapter: Dictionary containing adapter metadata with 'name' field
    """
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



def start_nicegui(port: int = DEFAULT_PORT) -> None:
    """
    Start the LEAF NiceGUI web interface.
    
    Creates a web-based interface for managing LEAF system configuration,
    viewing logs, managing adapters, and accessing documentation.
    
    Args:
        port: Port number to run the web server on (default: 8080)
    """
    ui.page('/')
    
    # Add custom CSS for enhanced styling and leaf favicon with dark mode support
    ui.add_head_html('''
        <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTggMTZDOCAxNiAxNiA8IDE2IDhDMTYgOCAyNCAxNiAyNCAxNkMyNCAxNiAxNiAyNCAxNiAyNEMxNiAyNCA4IDE2IDggMTZaIiBmaWxsPSIjNDA5NkZGIiBzdHJva2U9IiMyNTYzRUIiIHN0cm9rZS13aWR0aD0iMiIvPgo8L3N2Zz4K">
        <style>
            /* Light mode styles */
            .leaf-gradient {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            }
            .leaf-card {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.2);
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                border-radius: 12px;
                transition: all 0.3s ease;
            }
            .leaf-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 12px 48px rgba(0, 0, 0, 0.15);
            }
            .leaf-header {
                background: linear-gradient(135deg, #4CAF50 0%, #2E7D32 100%);
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
            }
            .leaf-tab {
                transition: all 0.2s ease;
                border-radius: 8px 8px 0 0;
            }
            .leaf-tab:hover {
                background: rgba(255, 255, 255, 0.1);
            }
            .status-indicator {
                width: 12px;
                height: 12px;
                border-radius: 50%;
                display: inline-block;
                margin-right: 8px;
                animation: pulse 2s infinite;
            }
            .status-online { background-color: #4CAF50; }
            .status-offline { background-color: #f44336; }
            .status-warning { background-color: #ff9800; }
            
            /* Dark mode styles */
            .dark .leaf-card {
                background: rgba(31, 41, 55, 0.95);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(55, 65, 81, 0.4);
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            }
            .dark .leaf-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 12px 48px rgba(0, 0, 0, 0.4);
                border: 1px solid rgba(75, 85, 99, 0.6);
            }
            .dark .leaf-header {
                background: linear-gradient(135deg, #059669 0%, #065f46 100%);
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
            }
            .dark .leaf-tab:hover {
                background: rgba(0, 0, 0, 0.2);
            }
            
            /* Enhanced animations */
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }
            
            /* Dark mode tab styling */
            .dark .q-tabs {
                background: linear-gradient(135deg, #1f2937 0%, #111827 100%) !important;
            }
            
            /* Dark mode button styling */
            .dark .bg-gray-50 {
                background-color: #1f2937 !important;
            }
            .dark .border-t {
                border-color: #374151 !important;
            }
            
            /* Dark mode help cards */
            .dark .bg-blue-50 {
                background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%) !important;
                border-color: #3b82f6 !important;
            }
            .dark .bg-green-50 {
                background: linear-gradient(135deg, #065f46 0%, #047857 100%) !important;
                border-color: #10b981 !important;
            }
            .dark .bg-amber-50 {
                background: linear-gradient(135deg, #92400e 0%, #b45309 100%) !important;
                border-color: #f59e0b !important;
            }
            
            /* Dark mode configuration help box */
            .dark .bg-gradient-to-br.from-blue-50.to-indigo-50 {
                background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%) !important;
                border-color: #3b82f6 !important;
            }
            .dark .text-gray-700 {
                color: #e5e7eb !important;
            }
            
            /* Dark mode text colors */
            .dark .text-gray-800 {
                color: #f3f4f6 !important;
            }
            .dark .text-gray-700 {
                color: #e5e7eb !important;
            }
            .dark .text-gray-600 {
                color: #d1d5db !important;
            }
        </style>
    ''')

    # Dark mode toggle
    dark = ui.dark_mode()

    # Enhanced header layout with leaf branding
    with ui.header().classes('leaf-header').style('padding: 16px; color: white;'):
        with ui.row().classes('justify-between items-center w-full'):
            with ui.row().classes('items-center'):
                import base64

                # Load SVG from images/icon.svg
                curr_dir: Path = Path(os.path.dirname(os.path.realpath(__file__)))
                svg_path: Path = curr_dir / "images" / "icon.svg"
                if os.path.exists(svg_path):
                    with open(svg_path, 'r') as svg_file:
                        svg = svg_file.read()
                        b64 = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
                        ui.html(f'<img width="32" height="32" src="data:image/svg+xml;base64,{b64}" />')

                ui.label('LEAF').classes('text-3xl font-bold ml-2')
                ui.label('Laboratory Equipment Adapter Framework').classes('text-sm opacity-80 ml-4 hidden md:block')

            with ui.row().classes('items-center'):
                # Status indicator
                with ui.row().classes('items-center mr-4 hidden md:flex'):
                    ui.html('<span class="status-indicator status-online"></span>')
                    ui.label('System Online').classes('text-sm opacity-90')
                
                # Enhanced dark mode toggle with better state management
                def toggle_dark_mode():
                    if dark.value:
                        dark.disable()
                        toggle_button.icon = 'light_mode'
                        toggle_button._props['title'] = 'Switch to Dark Mode'
                    else:
                        dark.enable()
                        toggle_button.icon = 'dark_mode'
                        toggle_button._props['title'] = 'Switch to Light Mode'

                toggle_button = ui.button('', 
                                        icon='light_mode', 
                                        on_click=toggle_dark_mode
                                        ).props('flat round title="Switch to Dark Mode"').classes(
                    'text-yellow-300 bg-transparent hover:bg-white/20 transition-all duration-300 transform hover:scale-110'
                )

    # Enhanced tabs with icons
    with ui.tabs().classes('w-full bg-gradient-to-r from-gray-50 to-white border-b') as tabs:
        config_tab: ui.tab = ui.tab('Configuration', icon='settings').classes('leaf-tab')
        logs_tab = ui.tab('Logs', icon='article').classes('leaf-tab')
        docs_tab = ui.tab('Documentation', icon='help').classes('leaf-tab')
        adapters_tab = ui.tab('Adapters', icon='extension').classes('leaf-tab')

    with ui.tab_panels(tabs, value=logs_tab).classes('w-full'):
        # Configuration tab
        with ui.tab_panel(config_tab).classes('w-full h-full'):
            # Header section
            with ui.row().classes('items-center mb-6 px-6 pt-6'):
                ui.icon('settings', size='2rem').classes('text-green-600')
                ui.label('LEAF Configuration').classes('text-2xl font-bold text-gray-800 ml-2')
            
            # Code editor for YAML
            curr_dir: Path = Path(os.path.dirname(os.path.realpath(__file__)))
            configuration_path: Path = curr_dir / ".." / 'config' / 'configuration.yaml'
            if os.path.exists(configuration_path):
                with open(configuration_path, 'r') as file:
                        config_yaml = file.read()
            else:
                logger.error("Configuration file not found")
                config_yaml = '''# No configuration file found'''

            # Full width layout for editor and help - using flex to prevent wrapping
            with ui.row().classes('w-full px-6 flex flex-nowrap'):
                # Configuration editor - Left side (flexible, takes remaining space)
                with ui.column().classes('flex-1 min-w-0 mr-6'):
                    ui.label('Configuration Editor').classes('text-lg font-semibold mb-3 text-gray-800')
                    with ui.card().classes('leaf-card h-96 w-full overflow-hidden'):
                        with ui.card_section().classes('p-0 w-full h-full'):
                            editor = ui.codemirror(value=config_yaml, language="YAML", theme='basicDark').classes('w-full h-full')

                # Help section - Right side (fixed width, no shrink)
                with ui.column().classes('w-[35%] flex-shrink-0'):
                    ui.label('Configuration Help').classes('text-lg font-semibold mb-3 text-gray-800')
                    with ui.card().classes('bg-gradient-to-br from-blue-50 to-indigo-50 border border-blue-200 h-96 overflow-y-auto shadow-sm'):
                        ui.markdown('''
                            ## Configuration Structure
                            
                            ### Equipment Instances
                            Define your laboratory equipment:
                            ```yaml
                            EQUIPMENT_INSTANCES:
                              - equipment:
                                  adapter: HelloWorld
                                  data:
                                    instance_id: my_device
                                    institute: university_lab
                                  requirements:
                                    interval: 30
                            ```
                            
                            ### Outputs
                            Configure data destinations:
                            ```yaml
                            OUTPUTS:
                              - plugin: KEYDB
                                host: localhost
                                port: 6379
                                fallback: FILE
                            ```
                            
                            ### Available Adapters
                            - **HelloWorld**: Demo adapter for testing
                            - Install more from the Adapters tab
                            
                            ### Pro Tips
                            - Use proper YAML indentation (2 spaces)
                            - Check logs for validation errors
                            - Test configurations incrementally
                            - Use fallback chains for reliability
                            ''').classes('p-4 text-sm text-gray-700')

            # Button to start/restart the adapters
            def restart_app(restart: bool) -> None:
                # Write new configuration to file
                if restart:
                    logger.debug("Writing new configuration to file... " + str(configuration_path) + " with content: " + editor.value)
                    with open(configuration_path, 'w') as config_file:
                        config_file.write(editor.value)
                    ui.notify('Configuration saved! Restarting...', icon='check_circle', color='positive')
                    logger.info("Restarting...")
                    os.execl(sys.executable, sys.executable, *sys.argv)
                else:
                    # Close the current window and shutdown the app
                    ui.notify('Stopping LEAF system...', icon='power_settings_new', color='negative')
                    ui.run_javascript('window.open(location.href, "_self", "");window.close()')
                    os.execl(sys.executable, sys.executable, sys.argv[0], "--shutdown")

            # Action buttons at the bottom - aligned with tab headers
            with ui.row().classes('w-full gap-4 py-4 border-t bg-gray-50 pl-4'):
                ui.button('Restart App', icon='refresh', on_click=partial(restart_app, True)).classes('bg-green-500 text-white px-6 py-3 rounded-lg hover:bg-green-600 transition-colors')
                ui.button('Stop App', icon='stop', on_click=partial(restart_app, False)).classes('bg-red-500 text-white px-6 py-3 rounded-lg hover:bg-red-600 transition-colors')


        # Logs tab
        with ui.tab_panel(logs_tab).classes('w-full p-6'):
            with ui.card().classes('leaf-card w-full'):
                with ui.card_section():
                    with ui.row().classes('items-center justify-between mb-4'):
                        with ui.row().classes('items-center'):
                            ui.icon('article', size='2rem').classes('text-blue-600')
                            ui.label('LEAF System Logs').classes('text-2xl font-bold text-gray-800 ml-2')
                        with ui.row().classes('items-center gap-2'):
                            ui.html('<span class="status-indicator status-online"></span>')
                            ui.label('Live Logging').classes('text-sm font-semibold text-green-600')
                    
                    # Log controls
                    with ui.row().classes('items-center gap-4 mb-4'):
                        def clear_logs():
                            log.clear()
                            ui.notify('Logs cleared', icon='clear_all', color='info')
                            logger.info("Log display cleared by user")
                        
                        def download_logs():
                            # This would be implemented to download actual log files
                            ui.notify('Log download feature coming soon', icon='download', color='info')
                        
                        ui.button('Clear', icon='clear_all', on_click=clear_logs).classes('bg-gray-500 text-white px-4 py-2 rounded hover:bg-gray-600 transition-colors')
                        ui.button('Download', icon='download', on_click=download_logs).classes('bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600 transition-colors')
                    
                    # Enhanced log display with better styling
                    log = ui.log(max_lines=MAX_LOG_LINES).classes('w-full h-96 bg-gray-900 text-green-400 font-mono text-sm p-4 rounded-lg border overflow-y-auto')
                    handler = LogElementHandler(log)
                    logger.addHandler(handler)
                    ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))
                    logger.info("🚀 Logger interface connected and ready!")

        # Adapters tab - Enhanced
        with ui.tab_panel(adapters_tab).classes('w-full p-6'):
            with ui.card().classes('leaf-card w-full'):
                with ui.card_section():
                    with ui.row().classes('items-center mb-6'):
                        ui.icon('extension', size='2rem').classes('text-purple-600')
                        ui.label('LEAF Adapter Management').classes('text-2xl font-bold text-gray-800 ml-2')
                    
                    # Installed Adapters Section
                    with ui.row().classes('items-center mb-4'):
                        ui.icon('inventory', size='1.5rem').classes('text-green-600')
                        ui.label('Installed Adapters').classes('text-xl font-semibold text-gray-700 ml-2')
                    
                    installed_adapters = get_all_adapter_codes()
                    if len(installed_adapters) > 0:
                        with ui.row().classes('w-full flex-wrap gap-4 mb-8'):
                            for installed_adapter in installed_adapters:
                                with ui.card().classes(f'{CARD_CLASSES} {CARD_WIDTH_CLASS}'):
                                    with ui.card_section():
                                        with ui.row().classes('items-center justify-between mb-2'):
                                            ui.icon('check_circle', color='green').classes('text-2xl')
                                            ui.chip('INSTALLED', color='green')
                                        ui.label(installed_adapter['code']).classes('text-lg font-bold text-gray-800 mb-1')
                                        ui.label(installed_adapter['name']).classes('text-sm text-gray-600 mb-3')
                                        
                                        def make_uninstall_handler(adapter):
                                            return lambda: uninstall_adapter(adapter)
                                        
                                        ui.button('Uninstall',
                                                icon='delete', 
                                                on_click=make_uninstall_handler(installed_adapter)).classes(
                                            'bg-red-500 text-white w-full rounded-lg hover:bg-red-600 transition-colors'
                                        )
                    else:
                        with ui.card().classes('bg-yellow-50 border border-yellow-200 p-4 mb-8'):
                            with ui.row().classes('items-center'):
                                ui.icon('warning', color='orange').classes('text-2xl mr-2')
                                ui.label('No adapters installed. Install adapters from the marketplace below.').classes('text-gray-600')

                    ui.separator().classes('my-6')

                    # Available Adapters Section
                    with ui.row().classes('items-center mb-4'):
                        ui.icon('store', size='1.5rem').classes('text-blue-600')
                        ui.label('Adapter Marketplace').classes('text-xl font-semibold text-gray-700 ml-2')
                    
                    try:
                        url = MARKETPLACE_URL
                        response = httpx.get(url)
                        adapter_content.clear()
                        data = response.json()
                        
                        with ui.row().classes('w-full flex-wrap gap-4'):
                            for adapter in data:
                                with ui.card().classes(f'{CARD_CLASSES} {CARD_WIDTH_CLASS}'):
                                    with ui.card_section():
                                        with ui.row().classes('items-center justify-between mb-2'):
                                            ui.icon('cloud_download', color='blue').classes('text-2xl')
                                            ui.chip('AVAILABLE', color='blue')
                                        ui.label(adapter['adapter_id']).classes('text-lg font-bold text-gray-800 mb-1')
                                        ui.label(adapter.get('name', 'No description')).classes('text-sm text-gray-600 mb-3')
                                        
                                        def make_install_handler(adptr):
                                            return lambda: install_adapter(adptr)
                                        
                                        ui.button('Install',
                                                icon='download', 
                                                on_click=make_install_handler(adapter)).classes(
                                            'bg-blue-500 text-white w-full rounded-lg hover:bg-blue-600 transition-colors'
                                        )
                    except Exception as e:
                        with ui.card().classes('bg-red-50 border border-red-200 p-4'):
                            with ui.row().classes('items-center'):
                                ui.icon('error', color='red').classes('text-2xl mr-2')
                                ui.label(f'Unable to load marketplace: {str(e)}').classes('text-red-600')

        # Documentation tab - Enhanced
        with ui.tab_panel(docs_tab).classes('w-full p-6'):
            with ui.card().classes('leaf-card w-full'):
                with ui.card_section():
                    with ui.row().classes('items-center mb-6'):
                        ui.icon('help', size='2rem').classes('text-indigo-600')
                        ui.label('LEAF Documentation').classes('text-2xl font-bold text-gray-800 ml-2')
                    
                    with ui.row().classes('w-full gap-6'):
                        # Main documentation - Left side (50%)
                        with ui.column().classes('w-45/100'):
                            ui.markdown('''
                            # LEAF System Overview
                            
                            **LEAF (Laboratory Equipment Adapter Framework)** is a powerful system for monitoring laboratory equipment and transmitting data to various cloud destinations.
                            
                            ## Quick Start Guide
                            
                            ### Step 1: Configure Your Setup
                            1. Navigate to the **Configuration** tab
                            2. Edit the YAML configuration to define your equipment and outputs
                            3. Save and restart the application
                            
                            ### Step 2: Install Adapters
                            1. Go to the **Adapters** tab
                            2. Browse available adapters in the marketplace
                            3. Install adapters for your specific equipment
                            
                            ### Step 3: Monitor Operations
                            1. Check the **Logs** tab for system activity
                            2. Monitor equipment status and data flow
                            3. Debug any issues using the live log feed
                            
                            ## Configuration Structure
                            
                            ### Equipment Instances
                            Define your laboratory equipment with specific adapters:
                            ```yaml
                            EQUIPMENT_INSTANCES:
                              - equipment:
                                  adapter: HelloWorld
                                  data:
                                    instance_id: my_bioreactor_01
                                    institute: university_lab
                                  requirements:
                                    interval: 30  # seconds
                            ```
                            
                            ### Output Destinations  
                            Configure where data should be sent:
                            ```yaml
                            OUTPUTS:
                              - plugin: MQTT
                                broker: localhost
                                port: 1883
                                fallback: KEYDB
                              
                              - plugin: KEYDB
                                host: localhost
                                port: 6379
                                db: 0
                                fallback: FILE
                            ```
                            
                            ## Available Adapters
                            
                            See the **Adapters** tab for a full list of available adapters and plugins.
                            
                            ## Use Cases
                            
                            - **Bioreactor Monitoring**: Track pH, temperature, dissolved oxygen
                            - **Analytical Instruments**: Connect HPLC, spectrophotometers
                            - **Environmental Monitoring**: Temperature, humidity sensors
                            - **Process Control**: Automated laboratory workflows
                            ''').classes('prose max-w-none text-sm')
                        
                        # Quick reference sidebar - Right side (50%)
                        with ui.column().classes('w-45/100'):
                            with ui.card().classes('bg-gradient-to-br from-blue-50 to-indigo-100 border border-blue-200 mb-4'):
                                with ui.card_section():
                                    ui.label('Quick Links').classes('text-lg font-bold text-indigo-800 mb-3')
                                    with ui.column().classes('gap-2'):
                                        ui.button('🌐 Official Documentation (Pop-up)',
                                                on_click=lambda: ui.run_javascript('window.open("https://leaf.systemsbiology.nl", "_blank")')).classes(
                                            'bg-indigo-500 text-white w-full rounded hover:bg-indigo-600 transition-colors'
                                        )
                                        ui.button('Adapter Templates (Pop-up)',
                                                on_click=lambda: ui.run_javascript('window.open("https://gitlab.com/LabEquipmentAdapterFramework/leaf-adapters/leaf-template", "_blank")')).classes(
                                            'bg-green-500 text-white w-full rounded hover:bg-green-600 transition-colors'
                                        )
                                        ui.button('Report Issues / Request Features (Pop-up)',
                                                on_click=lambda: ui.run_javascript('window.open("https://gitlab.com/LabEquipmentAdapterFramework/leaf/-/issues", "_blank")')).classes(
                                            'bg-orange-500 text-white w-full rounded hover:bg-orange-600 transition-colors'
                                        )
                            
                            with ui.card().classes('bg-gradient-to-br from-green-50 to-emerald-100 border border-green-200 mb-4'):
                                with ui.card_section():
                                    ui.label('⚡ System Status').classes('text-lg font-bold text-green-800 mb-3')
                                    with ui.column().classes('gap-2'):
                                        with ui.row().classes('items-center justify-between'):
                                            ui.label('Framework').classes('text-sm font-medium')
                                            ui.chip('ACTIVE', color='green')
                                        with ui.row().classes('items-center justify-between'):
                                            ui.label('Configuration').classes('text-sm font-medium')
                                            ui.chip('LOADED', color='blue')
                                        with ui.row().classes('items-center justify-between'):
                                            ui.label('Adapters').classes('text-sm font-medium')
                                            installed_count = len(get_all_adapter_codes())
                                            ui.chip(f'{installed_count} INSTALLED', color='purple')
                            
                            with ui.card().classes('bg-gradient-to-br from-amber-50 to-yellow-100 border border-amber-200'):
                                with ui.card_section():
                                    ui.label('💡 Pro Tips').classes('text-lg font-bold text-amber-800 mb-3')
                                    ui.markdown('''
                                    • **Test configurations** gradually by adding one equipment instance at a time
                                    
                                    • **Monitor logs** regularly for early issue detection
                                    
                                    • **Use fallback chains** in outputs for reliability
                                    
                                    • **Check adapter compatibility** before installation
                                    
                                    • **Backup configurations** before major changes
                                    ''').classes('text-sm text-amber-700')

    ui.run(reload=False, port=port)