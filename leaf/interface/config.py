import os

import yaml
from nicegui import ui
import asyncio

from leaf.start import run_adapters


def existing_yamls() -> list[str]:
    # Obtain current directory
    import os
    current_dir = os.getcwd()
    # Check for yaml files in current directory
    yaml_files = [f for f in os.listdir(current_dir) if f.endswith('.yaml')]
    # If the yaml file content contains 'EQUIPMENT_INSTANCES'
    yaml_files = [f for f in yaml_files if 'EQUIPMENT_INSTANCES' in open(f).read()]
    # If there are yaml files in the current directory
    if yaml_files:
        ui.notify(f'Found yaml files in current directory: {len(yaml_files)}', color='positive')
    yaml_files.insert(0, 'Select yaml file')
    return yaml_files


async def create_config_panel(self, config_tab) -> None:
    # Configuration tab
    file_selection = self.global_args.config if self.global_args is not None else ""
    if file_selection is None:
        file_selection = "No yaml files found"
    with ui.tab_panel(config_tab).style('width: 100%'):
        with ui.row().style("display: flex; width: 100%"):
            async def load_config(selection_path) -> None:
                if os.path.exists(selection_path.value):
                    ui.notify(f'Loading config from selection path')
                    path = selection_path.value
                    try:
                        with open(path, 'r') as file:
                            config = yaml.safe_load(file)
                            self.global_config = config
                            ui.notify(f'Configuration loaded: {path}', color='positive')
                            # Update the editor with the loaded config
                            # print(f'Dumping config:\n{yaml.dump(self.global_config, indent=4)}')
                            config_editor.value = yaml.dump(self.global_config, indent=4)
                    except Exception as e:
                        ui.notify(f'Error loading configuration: {str(e)}', color='negative')
                else:
                    if selection_path.value.endswith('.yaml'):
                        ui.notify(f'File not found', color='negative')

            # Configuration form
            try:
                ui.input(label='Load configuration from file path', value=self.global_args.config).style('width: 100%').on_value_change(load_config)
            except Exception as e:
                # TODO ensure global_args is not None
                ui.input(label='Load configuration from file path', value="").style('width: 100%').on_value_change(load_config)

            # Check for yaml files in current directory
            yaml_files = existing_yamls()
            if yaml_files:
                ui.select(yaml_files, value=yaml_files[0], label='Load yaml file from current directory').style('width: 100%').on_value_change(load_config)

        with ui.row().style("width: 100%"):
            # Configuration output
            ui.label('Configuration Output (configuration.yaml)').classes('text-xl font-bold')

            # Initially set the editor with any existing config data
            if self.global_config is None and self.global_args.config is not None:
                with open(self.global_args.config, 'r') as file:
                    self.global_config = yaml.safe_load(file)
            if self.global_config is None:
                config_editor = ui.codemirror(value="# No config file loaded yet",
                                              language="YAML").style('width: 100%')
            else:
                config_editor = ui.codemirror(value=yaml.dump(self.global_config), language="YAML").style('width: 100%')

            async def save_config(self, config_content) -> None:
                # path = config_path.value
                # if not path:
                #     ui.notify('Please enter a configuration file path', color='negative')
                #     return
                #
                # try:
                #     with open(path, 'w') as file:
                #         file.write(config_editor.value)
                #         ui.notify(f'Configuration saved: {path}', color='positive')
                #
                #    # Update global config from path
                #     with open(path, 'r') as file:
                    self.global_config = yaml.safe_load(config_content) # yaml.safe_load(file)

                    # Stop the running adapters asynchronously
                    ui.notify("Stopping adapters")
                    await asyncio.to_thread(self.stop_adapters_func)

                    # Uncomment and adjust the following if you need to restart adapters asynchronously
                    from leaf.error_handler.error_holder import ErrorHolder
                    from leaf.start import _get_output_module

                    general_error_holder = ErrorHolder()
                    output = _get_output_module(self.global_config, general_error_holder)

                    script_dir = os.path.dirname(os.path.realpath(__file__))
                    # Create config directory
                    global_configuration = os.path.join(script_dir, "..", "config", "configuration.yaml")
                    print(os.path.isfile(global_configuration))
                    run_adapters(equipment_instances=self.global_config["EQUIPMENT_INSTANCES"], output=output, error_handler=general_error_holder, external_adapter=global_configuration)
                    # await asyncio.to_thread(self.start_adapters_func, self.global_config["EQUIPMENT_INSTANCES"], output, general_error_holder)
                # except Exception as e:
                #     ui.notify(f'Error saving configuration: {str(e)}', color='negative')

            # Save button
            ui.button('Save Configuration', on_click=lambda _: save_config(self, config_editor.value)).style('width: 100%')