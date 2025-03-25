import asyncio
import logging
import os
import signal
import threading
from datetime import datetime
from typing import Callable, Any

from nicegui import ui

from leaf.interface.adapters import create_adapters_panel
from leaf.interface.config import create_config_panel
from leaf.interface.dashboard import create_dashboard_panel
from leaf.interface.docs import create_docs_panel
from leaf.interface.logs import create_logs_panel

# Initialize logger
logger = logging.getLogger(__name__)

class LEAFGUI:
    def __init__(self, port: int = 8080):
        self.port = port
        self.global_config = None
        self.global_output = None
        self.global_error_handler = None
        self.global_args = None
        self.leaf_state: dict[str, Any] = {
            "status": "Initializing",
            "active_adapters": 0,
            "errors": [],
            "warnings": []
        }
        
    def register_callbacks(self, 
                          start_adapters_func: Callable[[], None],
                          stop_adapters_func: Callable[[], None]) -> None:
        """Register callback functions from main module"""
        self.start_adapters_func = start_adapters_func
        self.stop_adapters_func = stop_adapters_func
        
    def start_adapters_background(self) -> bool:
        """Start all adapters using the global variables."""
        if self.global_output and self.global_config:
            ui.notify("Starting LEAF adapters...")
            # Run in a background task to avoid blocking the UI
            run_thread = threading.Thread(
                target=self.start_adapters_func,
                args=(self.global_config["EQUIPMENT_INSTANCES"], 
                     self.global_output, 
                     self.global_error_handler
                     # self.global_external_adapter
                      )
            )
            run_thread.daemon = True
            run_thread.start()
            return True
        else:
            ui.notify("Configuration not loaded yet", color="negative")
            return False


    async def setup_nicegui(self) -> None:
        """Set up NiceGUI web interface"""
        @ui.page('/')
        async def index() -> None:
            # Add favicon
            ui.add_head_html('<link rel="icon" type="image/x-icon" href="https://nicegui.io/favicon.ico">')
            
            # Main layout

            # Header layout
            async def confirm_exit() -> None:
                # TODO not working properly yet
                with ui.dialog() as dialog, ui.card():
                    ui.label("Are you sure you want to turn off?").classes('text-lg font-bold')
                    with ui.row().classes('justify-end'):
                        ui.button("Cancel", on_click=dialog.close).classes('bg-gray-500 text-white')
                        ui.button("Yes, Exit", on_click=lambda: os.kill(os.getpid(), signal.SIGKILL)).classes(
                            'bg-red-500 text-white')
                dialog.open()

            with ui.header().style('background-color: rgb(133, 171, 215); color: white; padding: 10px;'):
                with ui.row().classes('justify-between items-center w-full'):
                    ui.label('LEAF Monitoring System').classes('text-2xl font-bold')
                    # Kill button to stop the system (not working yet)
                    ui.button('Turn Off', on_click=confirm_exit).classes('bg-red-500 text-white').visible = False

            # Footer layout
            with ui.footer().classes('bg-gray-100 justify-center'):
                # Obtain current year
                year = str(datetime.now().year)
                ui.label(f'LEAF ({year})').classes('text-black')
                ui.link(text='leaf.systemsbiology.nl', target='https://leaf.systemsbiology.nl', new_tab=True).classes('text-black')

            # Tabs
            with ui.tabs().classes('w-full') as tabs:
                dashboard_tab = ui.tab('Dashboard')
                config_tab = ui.tab('Configuration')
                logs_tab = ui.tab('Logs')
                adapters_tab = ui.tab('Adapters')
                docs_tab = ui.tab('Documentation')

            # Main content for all tabs
            with ui.tab_panels(tabs, value=dashboard_tab).classes('w-full'):
                # Add the dashboard panel
                create_dashboard_panel(self, dashboard_tab)
                
                # Other tab panels would follow...
                with ui.tab_panel(config_tab):
                    # Configuration tab content
                    await create_config_panel(self, config_tab)

                with ui.tab_panel(logs_tab):
                    # Logs tab content
                    create_logs_panel()

                with ui.tab_panel(adapters_tab):
                    # Adapters tab content
                    await create_adapters_panel(tabs, adapters_tab, self)
            
                with ui.tab_panel(docs_tab):
                    # Documentation tab content
                    create_docs_panel(tabs, docs_tab, self)

    # def update_error_state(self, error, severity: SeverityLevel) -> None:
    #     """Update the UI state with new errors or warnings"""
    #     if severity in [SeverityLevel.CRITICAL, SeverityLevel.ERROR]:
    #         self.leaf_state["errors"].append(str(error))
    #         if len(self.leaf_state["errors"]) > 10:
    #             self.leaf_state["errors"].pop(0)
    #     elif severity == SeverityLevel.WARNING:
    #         self.leaf_state["warnings"].append(str(error))
    #         if len(self.leaf_state["warnings"]) > 10:
    #             self.leaf_state["warnings"].pop(0)
    
    def update_status(self, status: str) -> None:
        """Update the UI status"""
        self.leaf_state["status"] = status

    # def update_adapters_count(self, count: int) -> None:
    #     """Update the active adapters count"""
    #     self.leaf_state["active_adapters"] = count

    def run(self) -> None:
        """Start the NiceGUI interface"""
        # self.setup_nicegui()
        asyncio.run(self.setup_nicegui())  # ✅ Run the async function properly
        ui.run(port=self.port, title="LEAF Monitoring System", show=False)


# Function to create and return a GUI instance
def create_gui(port: int = 8080) -> LEAFGUI:
    return LEAFGUI(port)
