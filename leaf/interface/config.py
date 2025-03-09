import yaml
from nicegui import ui
import asyncio


async def create_config_panel(tabs, config_tab, self) -> None:
    # Configuration tab
    with ui.tab_panel(config_tab).style('width: 100%'):
        with ui.row().style("display: flex; width: 100%"):
            # Configuration form
            config_path = ui.input(label='Configuration File Path', value=self.global_args.config).style('width: 100%')

            async def load_config() -> None:
                path = config_path.value
                if not path:
                    ui.notify('Please enter a configuration file path', color='negative')
                    return

                try:
                    with open(path, 'r') as file:
                        config = yaml.safe_load(file)
                        self.global_config = config
                        ui.notify(f'Configuration loaded: {path}', color='positive')

                        # Update the editor with the loaded config
                        print(f'Dumping config:\n{yaml.dump(self.global_config, indent=4)}')
                        config_editor.value = yaml.dump(self.global_config, indent=4)
                except Exception as e:
                    ui.notify(f'Error loading configuration: {str(e)}', color='negative')

            ui.button('Load Configuration', on_click=load_config)

        with ui.row().style("width: 100%"):
            # Configuration output
            ui.label('Configuration Output').classes('text-xl font-bold')

            # Initially set the editor with any existing config data
            if self.global_config is None:
                config_editor = ui.codemirror(value="# Click load configuration to obtain its content",
                                              language="YAML").style('width: 100%')
            else:
                config_editor = ui.codemirror(value=yaml.dump(self.global_config), language="YAML").style('width: 100%')

            async def save_config() -> None:
                path = config_path.value
                if not path:
                    ui.notify('Please enter a configuration file path', color='negative')
                    return

                try:
                    with open(path, 'w') as file:
                        file.write(config_editor.value)
                        ui.notify(f'Configuration saved: {path}', color='positive')

                    # Update global config from path
                    with open(path, 'r') as file:
                        self.global_config = yaml.safe_load(file)

                    # Stop the running adapters asynchronously
                    ui.notify("Stopping adapters")
                    await asyncio.to_thread(self.stop_adapters_func)

                    # Uncomment and adjust the following if you need to restart adapters asynchronously
                    from leaf.error_handler.error_holder import ErrorHolder
                    from leaf.start import _get_output_module

                    general_error_holder = ErrorHolder()
                    output = _get_output_module(self.global_config, general_error_holder)
                    await asyncio.to_thread(self.start_adapters_func, self.global_config["EQUIPMENT_INSTANCES"], output, general_error_holder)

                except Exception as e:
                    ui.notify(f'Error saving configuration: {str(e)}', color='negative')

            # Save button
            ui.button('Save Configuration', on_click=save_config)