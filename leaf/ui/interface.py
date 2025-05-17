import logging

from leaf.registry.discovery import get_all_adapter_codes
from nicegui import ui

logger = logging.getLogger()

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

def start_nicegui():
    ui.page('/')
    
      # Add favicon
    ui.add_head_html('<link rel="icon" type="image/x-icon" href="https://nicegui.io/favicon.ico">')

    # Header layout
    with ui.header().style('background-color: rgb(133, 171, 215); color: white; padding: 10px;'):
        with ui.row().classes('justify-between items-center w-full'):
            ui.label('LEAF Monitoring System').classes('text-2xl font-bold')

    # Tabs
    with ui.tabs().classes('w-full') as tabs:
        config_tab: ui.tab = ui.tab('Configuration')
        logs_tab = ui.tab('Logs')
        docs_tab = ui.tab('Documentation')
        adapters_tab = ui.tab('Adapters')

    with ui.tab_panels(tabs, value=logs_tab).classes('w-full'):
        # Configuration tab
        with ui.tab_panels(tabs, value=config_tab).classes('w-full'):
            ui.label('LEAF Configuration').classes('text-xl font-bold')

        # Logs tab
        with ui.tab_panel(logs_tab):
            ui.label('LEAF Logs').classes('text-xl font-bold')
            log = ui.log(max_lines=10).classes('w-full')
            handler = LogElementHandler(log)
            logger.addHandler(handler)
            ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))
            logger.info("bla?")

        # Plugins tab
        with ui.tab_panel(adapters_tab):
            # Obtain a list of all available adapters
            adapter_codes = get_all_adapter_codes()
            # Create a list of adapter names
            ui.label("Available Adapters:").classes('text-xl font-bold')
            for adapter_code in adapter_codes:
                # Create a button for each adapter
                ui.label(adapter_code).classes('text-xl font-bold')
            # Create a dialog to install adapters
            with ui.dialog() as install_adapters, ui.card():
                ui.label('Hello world!')
                ui.button('Close', on_click=install_adapters.close)

            # Button to open the dialog
            ui.button('Install Adapters', on_click=install_adapters.open).classes('bg-blue-500 text-white font-bold py-2 px-4 rounded')

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

    ui.run(reload=False)