import os
import time
import unittest
import subprocess
import sys
import threading

import requests

import leaf.start


class TestStartup(unittest.TestCase):
    def check_nicegui_status(self, url: str, timeout: int = 10) -> bool:
        """Wait until NiceGUI is available or timeout."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(url, timeout=3)
                if response.status_code == 200:
                    print("NiceGUI is running successfully!")
                    return True
            except requests.exceptions.RequestException:
                pass  # Server not up yet, retry

            time.sleep(1)  # Wait before retrying

        print("NiceGUI is not reachable.")
        return False

    @unittest.skipIf(os.getenv("CI") == "true", reason="Skipping test in GitLab Runner")
    def test_startup(self) -> None:
        """Test if NiceGUI starts successfully."""
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        process = subprocess.Popen(
            ["python", "-u", curr_dir + "/../leaf/start.py", "--config", curr_dir + "/example.yaml"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Function to stream output safely
        def stream_output(pipe, output_func) -> None:
            try:
                for line in iter(pipe.readline, ''):
                    output_func(line)
                    sys.stdout.flush()
            except Exception as e:
                print(f"Error streaming output: {e}")

        stdout_thread = threading.Thread(target=stream_output, args=(process.stdout, sys.stdout.write), daemon=True)
        stderr_thread = threading.Thread(target=stream_output, args=(process.stderr, sys.stderr.write), daemon=True)

        stdout_thread.start()
        stderr_thread.start()

        try:
            # Wait for NiceGUI to start properly
            time.sleep(1000000) # Sleep for a bit for adapters to start
            status = self.check_nicegui_status("http://127.0.0.1:8080", timeout=10)
            self.assertTrue(status, "NiceGUI did not start successfully")
        except subprocess.TimeoutExpired:
            print("Process timed out and will be terminated")

        finally:
            # Ensure the process is terminated
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

        # Give threads time to finish processing
        stdout_thread.join(1)
        stderr_thread.join(1)

        # Ensure process exited successfully
        # self.assertEqual(process.returncode, 0, "The process exited with an error")
        # TODO - Fix the adapter shutdown issue

    # @unittest.skipIf(os.getenv("CI") == "true", reason="Skipping test in GitLab Runner")
    # def test_start_forever(self) -> None:
    #     """Test that the GUI starts without blocking the test."""
        # process = subprocess.Popen(["python", "-m", "leaf.start"])
        #
        # try:
        #     # Give it some time to start
        #     process.wait(timeout=5000)
        # except subprocess.TimeoutExpired:
        #     # If it's still running, assume it started successfully
        #     process.terminate()
        #     assert True
        # else:
        #     assert False, "NiceGUI process exited unexpectedly"
        #
        # threading.Thread(
        #     target=leaf.start.main,
        #     args=(["--config", "../leaf/example.yaml"],),
        #     daemon=True
        # ).start()
        # time.sleep(10000)

if __name__ == "__main__":
    import unittest
    unittest.main()