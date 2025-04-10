import logging
from typing import Callable

from nicegui import ui

from leaf.interface.logs import LogElementHandler
from leaf.start import AppContext
from leaf.start import context

# Initialize logger
logger = logging.getLogger(__name__)

class LEAFGUI:
    def __init__(self, context: AppContext) -> None:
        self.context = context
        self.port = self.context.args.port
        self.start_adapters_func = None
        self.stop_adapters_func = None

    def run(self) -> None:
        """Start the NiceGUI interface"""
        # Start the NiceGUI application
        ui.run(port=self.port, title="LEAF Monitoring System", show=True)

    def register_callbacks(self,
                          start_adapters_func: Callable[[], None],
                          stop_adapters_func: Callable[[], None]) -> None:
        """Register callback functions from main module"""
        self.start_adapters_func = start_adapters_func
        self.stop_adapters_func = stop_adapters_func


@ui.page('/')
def index() -> None:
    # Add favicon
    ui.add_head_html('<link rel="icon" type="image/x-icon" href="https://nicegui.io/favicon.ico">')

    # Layout

    # Header layout
    with ui.header().style('background-color: rgb(133, 171, 215); color: white; padding: 10px;'):
        with ui.row().classes('justify-between items-center w-full'):
            ui.label('LEAF Monitoring System').classes('text-2xl font-bold')

    # Tabs
    with ui.tabs().classes('w-full') as tabs:
        config_tab: ui.tab = ui.tab('Configuration')
        logs_tab = ui.tab('Logs')
        docs_tab = ui.tab('Documentation')

    with ui.tab_panels(tabs).classes('w-full'):
        # Logging tab
        with ui.tab_panel(logs_tab):
            log: ui.log = ui.log(max_lines=1000).classes('width-full h-[60vh] overflow-auto')
            log.push("Starting LEAF Monitoring System...")
            handler = LogElementHandler(log)

            # Define the log format you want (including date, class, level, and message)
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            formatter = logging.Formatter(log_format)

            # Set the formatter for the handler
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)

            # Add the handler to all available loggers
            for logger_name in logging.root.manager.loggerDict:
                logger = logging.getLogger(logger_name)

                # Check if the logger already has handlers to avoid adding the same handler multiple times
                if not logger.hasHandlers():
                    logger.addHandler(handler)
                else:
                    # Optional: Remove existing handler if it's the same type and re-add your custom handler
                    for existing_handler in logger.handlers:
                        if isinstance(existing_handler, LogElementHandler):
                            logger.removeHandler(existing_handler)
                            break
                    logger.addHandler(handler)

            # Log an initial message to indicate log collection is starting
            logger = logging.getLogger()  # This will use the root logger
            logger.info("Collecting logs up to 1000 lines")

            # Clean up the handler when the client disconnects
            ui.context.client.on_disconnect(lambda: logger.removeHandler(handler))

        ui.label('LEAF Documentation').classes('text-xl font-bold')
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

        with ui.tab_panel(config_tab).style('width: 100%'):
            with ui.row().style("display: flex; width: 100%"):
                # Configuration output
                ui.label('Configuration Output (configuration.yaml)').classes('text-xl font-bold')

                # Initially set the editor with any existing config data
                config_yaml = context.config_yaml
                config_editor = ui.codemirror(value=config_yaml, language="YAML").style('width: 100%')


    # def setup_nicegui(self) -> None:
    #     """Set up NiceGUI web interface"""


    #     # self.context = context
    #     # self.global_config = None
    #     # self.global_output = None
    #     # self.global_error_handler = None
    #     # self.global_args: argparse.Namespace|None = None
    #     self.stop_adapters_func: Callable[[], None]
    #     self.start_adapters_func: Callable[[], None]
    #     self.leaf_state: dict[str, Any] = {
    #         "status": "Initializing",
    #         "active_adapters": 0,
    #         "errors": [],
    #         "warnings": []
    #     }
    #

    # def start_adapters_background(self) -> bool:
    #     """Start all adapters using the global variables."""
    #     if self.global_output and self.global_config:
    #         ui.notify("Starting LEAF adapters...")
    #         # Run in a background task to avoid blocking the UI
    #         run_thread = threading.Thread(
    #             target=self.start_adapters_func,
    #             args=(self.global_config["EQUIPMENT_INSTANCES"],
    #                  self.global_output,
    #                  self.global_error_handler
    #                  # self.global_external_adapter
    #                   )
    #         )
    #         run_thread.daemon = True
    #         run_thread.start()
    #         return True
    #     else:
    #         ui.notify("Configuration not loaded yet", color="negative")
    #         return False
    #
    # def setup_nicegui(self) -> None:
    #     """Set up NiceGUI web interface"""
    #     @ui.page('/')
    #     def index() -> None:
    #         # Add favicon
    #         ui.add_head_html('<link rel="icon" type="image/x-icon" href="https://nicegui.io/favicon.ico">')
    #
    #         # Main layout
    #
    #         # Header layout
    # #         async def confirm_exit() -> None:
    # #             # TODO not working properly yet
    # #             with ui.dialog() as dialog, ui.card():
    # #                 ui.label("Are you sure you want to turn off?").classes('text-lg font-bold')
    # #                 with ui.row().classes('justify-end'):
    # #                     ui.button("Cancel", on_click=dialog.close).classes('bg-gray-500 text-white')
    # #                     ui.button("Yes, Exit", on_click=force_exit()).classes('bg-red-500 text-white')
    # #             dialog.open()
    # #
    #         with ui.header().style('background-color: rgb(133, 171, 215); color: white; padding: 10px;'):
    #             with ui.row().classes('justify-between items-center w-full'):
    #                 ui.label('LEAF Monitoring System').classes('text-2xl font-bold')
    #                 # Kill button to stop the system (not working yet)
    #                 ui.button('Turn Off', on_click=confirm_exit).classes('bg-red-500 text-white').visible = False
    #
    #         # Footer layout
    #         with ui.footer().classes('bg-gray-100 justify-center'):
    #             # Obtain current year
    #             year = str(datetime.now().year)
    #             ui.label(f'LEAF ({year})').classes('text-black')
    #             ui.link(text='leaf.systemsbiology.nl', target='https://leaf.systemsbiology.nl', new_tab=True).classes('text-black')
    #
    #         # Tabs
    #         with ui.tabs().classes('w-full') as tabs:
    #             dashboard_tab: ui.tab = ui.tab('Dashboard')
    #             config_tab: ui.tab = ui.tab('Configuration')
    #             # logs_tab = ui.tab('Logs')
    #             # adapters_tab = ui.tab('Adapters')
    #             # docs_tab = ui.tab('Documentation')
    #
    #         # Main content for all tabs
    #         with ui.tab_panels(tabs, value=dashboard_tab).classes('w-full'):
    #             # Add the dashboard panel
    #             # create_dashboard_panel(self, dashboard_tab)
    #
    #             # Other tab panels would follow...
    #             with ui.tab_panel(config_tab):
    #                 # Configuration tab content
    #                 await self.create_config_panel(config_tab)
    #
    #             # with ui.tab_panel(logs_tab):
    #             #     # Logs tab content
    #             #     create_logs_panel()
    #             #
    #             # with ui.tab_panel(adapters_tab):
    #             #     # Adapters tab content
    #             #     await create_adapters_panel(tabs, adapters_tab, self)
    #             #
    #             # with ui.tab_panel(docs_tab):
    #             #     # Documentation tab content
    #             #     create_docs_panel(tabs, docs_tab, self)
    #
    # def update_status(self, status: str) -> None:
    #     """Update the UI status"""
    #     self.leaf_state["status"] = status
    #
    # def update_adapters_count(self, count: int) -> None:
    #     """Update the active adapters count"""
    #     self.leaf_state["active_adapters"] = count
    #
    # async def create_config_panel(self, config_tab: ui.tab) -> None:
    #     # Configuration tab
    #     from leaf.start import context
    #     # file_selection = context.args.config if context.args.config is not None else ""
    #     # if file_selection is None:
    #         # file_selection = "No yaml files found"
    #     with ui.tab_panel(config_tab).style('width: 100%'):
    #         # with ui.row().style("display: flex; width: 100%"):
    #             # async def load_config(selection_path) -> None:
    #             #     if os.path.exists(selection_path.value):
    #             #         ui.notify(f'Loading config from selection path')
    #             #         path = selection_path.value
    #             #         try:
    #             #             with open(path, 'r') as file:
    #             #                 self.global_config = yaml.safe_load(file)
    #             #                 ui.notify(f'Configuration loaded: {path}', color='positive')
    #             #                 # Update the editor with the loaded config
    #             #                 # print(f'Dumping config:\n{yaml.dump(self.global_config, indent=4)}')
    #             #                 config_editor.value = yaml.dump(self.global_config, indent=4)
    #             #         except Exception as e:
    #             #             ui.notify(f'Error loading configuration: {str(e)}', color='negative')
    #             #     else:
    #             #         if selection_path.value.endswith('.yaml'):
    #             #             ui.notify(f'File not found', color='negative')
    #
    #             # Configuration form
    #             # try:
    #             #     ui.input(label='Load configuration from file path', value=self.global_args.config).style('width: 100%').on_value_change(load_config)
    #             # except Exception as e:
    #             #     # TODO ensure global_args is not None
    #             #     ui.input(label='Load configuration from file path', value="").style('width: 100%').on_value_change(load_config)
    #
    #             # Check for yaml files in current directory
    #             # yaml_files = existing_yamls()
    #             # if yaml_files:
    #             #     ui.select(yaml_files, value=yaml_files[0], label='Load yaml file from current directory').style('width: 100%').on_value_change(load_config)
    #
    #         with ui.row().style("width: 100%"):
    #             # Configuration output
    #             ui.label('Configuration Output (configuration.yaml)').classes('text-xl font-bold')
    #
    #             # Initially set the editor with any existing config data
    #             # if self.global_args and self.global_args.config:
    #             #     with open(self.global_args.config, 'r') as file:
    #             #         self.global_config = yaml.safe_load(file)
    #                     # config_editor = ui.codemirror(value=yaml.dump(self.global_config, indent=4), language="YAML").style('width: 100%')
    #             # else:
    #             #     config_editor = ui.codemirror(value="No configuration loaded", language="YAML").style('width: 100%')
    #             # else:
    #                 # config_editor = ui.codemirror(value=yaml.dump(self.global_config), language="YAML").style('width: 100%')
    #
    #             async def save_config(config_content: str) -> None:
    #                 # path = config_path.value
    #                 # if not path:
    #                 #     ui.notify('Please enter a configuration file path', color='negative')
    #                 #     return
    #                 #
    #                 # try:
    #                 #     with open(path, 'w') as file:
    #                 #         file.write(config_editor.value)
    #                 #         ui.notify(f'Configuration saved: {path}', color='positive')
    #                 #
    #                 #    # Update global config from path
    #                 #     with open(path, 'r') as file:
    #                     from leaf.start import context
    #                     context.config = yaml.safe_load(config_content)
    #                     context.config_yaml = config_content
    #
    #                     # Stop the running adapters asynchronously
    #                     ui.notify("Stopping adapters")
    #                     await asyncio.to_thread(self.stop_adapters_func)
    #
    #                     # Uncomment and adjust the following if you need to restart adapters asynchronously
    #                     # from leaf.error_handler.error_holder import ErrorHolder
    #                     # from leaf.start import _get_output_module
    #                     # from leaf.start import context
    #                     # general_error_holder = ErrorHolder()
    #                     # TODO find function as it changed in new update
    #                     # output = _get_output_module(self.global_config, general_error_holder)
    #                     # output = build_output_module(yaml.safe_load(context.config_yaml), context.error_handler)
    #
    #                     # script_dir = os.path.dirname(os.path.realpath(__file__))
    #                     # Create config directory
    #                     # global_configuration = os.path.join(script_dir, "..", "config", "configuration.yaml")
    #                     # print(f"GLOBAL CONFIG FILE EXISTS? {os.path.isfile(global_configuration)}")
    #                     # run_adapters(equipment_instances=self.global_config["EQUIPMENT_INSTANCES"], output=output, error_handler=general_error_holder, external_adapter=global_configuration)
    #                     # await asyncio.to_thread(self.start_adapters_func, self.global_config["EQUIPMENT_INSTANCES"], output, general_error_holder)
    #                 # except Exception as e:
    #                 #     ui.notify(f'Error saving configuration: {str(e)}', color='negative')
    #
    #             # Save button
    #             ui.button('Save Configuration', on_click=lambda _: save_config(context, config_editor.value)).style('width: 100%')
