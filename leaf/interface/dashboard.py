from datetime import datetime
import nicegui.ui as ui

def create_dashboard_panel(tabs, dashboard_tab, leaf_state, self) -> None:
    """Create and populate the dashboard tab panel"""
    with ui.tab_panel(dashboard_tab):
        with ui.row().classes('w-full'):
            with ui.card().classes('w-full'):
                ui.label('System Status').classes('text-xl font-bold')
                
                # Status display that updates in real-time
                status_label = ui.label().bind_text_from(leaf_state, 'status')
                
                # Add active adapters count
                with ui.row():
                    ui.label('Active Adapters:')
                    ui.label().bind_text_from(leaf_state, 'active_adapters')
        
        with ui.row().classes('w-full'):
            # Errors panel               
            with ui.card().classes('w-1/2'):
                # Title
                ui.label('Recent Errors').classes('text-lg font-bold text-red-500')
                
                # Create a list for errors
                error_list = ui.list().props('dense separator').classes('w-full')
                
                # Function to update errors display
                def update_errors():
                    # Clear the current list
                    error_list.clear()
                    
                    # Add errors or "No errors" message
                    if not leaf_state['errors']:
                        with error_list:
                            dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            ui.item(f'No errors ({dt})').classes('text-gray-500')
                    else:
                        with error_list:
                            for error in leaf_state['errors']:
                                ui.item(error).classes('text-red-500 text-sm')
                
                # Initial display
                update_errors()
                
                # Set up timer to refresh errors
                ui.timer(3, update_errors)
            # Warnings panel
            with ui.card().classes('w-1/2'):
                ui.label('Recent Warnings').classes('text-lg font-bold text-amber-500')
                warnings_container = ui.column().classes('w-full')
                
                # Create a list for errors
                error_list = ui.list().props('dense separator').classes('w-full')
                
                # Function to update errors display
                def update_warnings():
                    # Clear the current list
                    error_list.clear()
                    
                    # Add errors or "No errors" message
                    if not leaf_state['warnings']:
                        with error_list:
                            dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            ui.item(f'No warnings ({dt})').classes('text-gray-500')
                    else:
                        with error_list:
                            for error in leaf_state['warnings']:
                                ui.item(error).classes('text-red-500 text-sm')
                
                # Set up timer to refresh warnings
                ui.timer(3, update_warnings)
        
        with ui.row().classes('w-full mt-4'):
            with ui.card().classes('w-full'):
                ui.label('Adapter Management').classes('text-xl font-bold')
                
                # Actions
                with ui.row():
                    def restart_adapters():
                        if self.stop_adapters_func:
                            self.stop_adapters_func()
                        # Start adapters again
                        self.start_adapters_background()
                        ui.notify('Restart operation triggered')
                    
                    ui.button('Start/Restart Adapters', on_click=restart_adapters).classes('bg-blue-500')
                    
                    def stop_system() -> None:
                        if self.stop_adapters_func:
                            self.stop_adapters_func()
                            ui.notify('System stopped')
                        else:
                            ui.notify('Stop function not registered', color='negative')
                    
                    ui.button('Stop System', on_click=stop_system).classes('bg-red-500')