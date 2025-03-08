from nicegui import ui


def create_logs_panel(tabs, logs_tab, self) -> None:
    with ui.tab_panel(logs_tab).style('width: 100%'):
        ui.label('System Logs').classes('text-xl font-bold')
