from nicegui import ui


def create_docs_panel(tabs, docs_tab, self) -> None:
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
