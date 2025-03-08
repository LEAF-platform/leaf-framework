import unittest
import subprocess
import sys
import threading


class TestStartup(unittest.TestCase):
    def test_startup(self) -> None:
        # Start the start.py script as a separate process
        process = subprocess.Popen(
            ["python", "../leaf/start.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line-buffered output
            universal_newlines=True  # Ensure text mode
        )

        # Function to stream output from a specific pipe
        def stream_output(pipe, output_func):
            for line in pipe:
                output_func(line)
                sys.stdout.flush()  # Ensure output is displayed immediately

        # Create and start threads for stdout and stderr
        stdout_thread = threading.Thread(
            target=stream_output,
            args=(process.stdout, lambda x: sys.stdout.write(x))
        )
        stderr_thread = threading.Thread(
            target=stream_output,
            args=(process.stderr, lambda x: sys.stderr.write(x))
        )

        stdout_thread.daemon = True  # Allow the program to exit even if threads are running
        stderr_thread.daemon = True

        stdout_thread.start()
        stderr_thread.start()

        # Wait for the process to complete or timeout
        try:
            process.wait(timeout=30)  # TODO build proper tests
        except subprocess.TimeoutExpired:
            print("Process timed out and will be terminated")
        finally:
            # Ensure the process is terminated
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()  # Force kill if it doesn't terminate gracefully
                process.wait()

        # Give threads a moment to finish processing any remaining output
        stdout_thread.join(1)
        stderr_thread.join(1)

        # Check if the process exited successfully
        self.assertEqual(process.returncode, 0, "The process exited with an error")