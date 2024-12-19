import threading
import time

def handle_disabled_modules(output,timeout):
    if (not output.is_enabled() and 
        time.time() - output.get_disabled_time() > timeout):
        output.enable()
        output.connect()
        connect_timeout_count = 0
        connect_timeout = 15
        while not output.is_connected():
            time.sleep(0.1)
            connect_timeout_count += 1
            if connect_timeout_count > connect_timeout:
                output.disable()
                return

        thread = threading.Thread(target=output_messages,
                                args=(output,))
        thread.daemon = True
        thread.start()

def output_messages(output_module):
    for topic,message in output_module.pop_all_messages():
        output_module.transmit(topic,message)