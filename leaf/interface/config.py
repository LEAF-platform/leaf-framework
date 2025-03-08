import yaml
from nicegui import ui

from leaf.error_handler.error_holder import ErrorHolder


def create_config_panel(tabs, config_tab, self) -> None:
    # Configuration tab
    with ui.tab_panel(config_tab).style('width: 100%'):
        with ui.row().style("display: flex; width: 100%"):
            # Configuration form
            config_path = ui.input(label='Configuration File Path', value=self.global_args.config).style('width: 100%')

            def load_config() -> None:
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
                        print(f'Dumping config: {yaml.dump(self.global_config, indent=4)}')
                        config_editor.value = yaml.dump(self.global_config, indent=4)

                        # Enable start button
                        ui.notify('System ready to start', color='positive')
                except Exception as e:
                    ui.notify(f'Error loading configuration: {str(e)}', color='negative')

            ui.button('Load Configuration', on_click=load_config)

        with ui.row().style("width: 100%"):
            # Configuration output
            ui.label('Configuration Output').classes('text-xl font-bold')
            # Initially set the editor with any existing config data
            config_editor = ui.codemirror(value=yaml.dump(self.global_config)).style('width: 100%')
            # Add a button to save the config to the config file
            def save_config() -> None:
                path = config_path.value
                if not path:
                    ui.notify('Please enter a configuration file path', color='negative')
                    return

                try:
                    with open(path, 'w') as file:
                        file.write(config_editor.value)
                        ui.notify(f'Configuration saved: {path}', color='positive')
                    # Update global config from path
                    self.global_config = yaml.load(open(path, 'r').read(), Loader=yaml.SafeLoader)
                    # Stop the running adapters
                    self.stop_adapters_func()
                    # Start the adapters
                    self.start_adapters_func(self.global_config["EQUIPMENT_INSTANCES"], self.global_output, ErrorHolder())
                except Exception as e:
                    ui.notify(f'Error saving configuration: {str(e)}', color='negative')

            # Save button
            ui.button('Save Configuration', on_click=save_config)