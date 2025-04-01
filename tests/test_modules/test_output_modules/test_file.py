import sys
import os
import unittest
from unittest.mock import mock_open
from unittest.mock import patch
import json
import uuid

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.output_modules.file import FILE


class TestFILEOutputModule(unittest.TestCase):

    def setUp(self):
        self.filename = f"test_{uuid.uuid4()}.json"

    def tearDown(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def test_transmit_creates_file_with_new_topic(self):
        topic = "test_topic"
        data = "test_data"

        with patch("builtins.open", mock_open()) as mock_open_func, \
             patch("os.path.exists", return_value=False):
            file_module = FILE(filename=self.filename)
            file_module.transmit(topic, data)

            mock_open_func.assert_called_once_with(self.filename, "w")

            handle = mock_open_func()
            written_data = "".join(call.args[0] for call in handle.write.call_args_list)
            expected_data = json.dumps({topic: [data]}, indent=4)
            self.assertEqual(written_data, expected_data)

    def test_transmit_appends_data_to_existing_topic(self):
        topic = "existing_topic"
        data = "new_data"

        with patch("builtins.open", mock_open(read_data='{"existing_topic": ["existing_data"]}')) as mock_open_func, \
             patch("os.path.exists", return_value=True):
            file_module = FILE(filename=self.filename)
            file_module.transmit(topic, data)

            mock_open_func.assert_any_call(self.filename, "r")
            mock_open_func.assert_any_call(self.filename, "w")

            handle = mock_open_func()
            written_data = "".join(call.args[0] for call in handle.write.call_args_list)
            expected_data = json.dumps(
                {"existing_topic": ["existing_data", "new_data"]}, indent=4
            )
            self.assertEqual(written_data, expected_data)

    def test_transmit_converts_existing_non_list_to_list(self):
        topic = "existing_topic"
        data = "new_data"

        with patch("builtins.open", mock_open(read_data='{"existing_topic": "string_value"}')) as mock_open_func, \
             patch("os.path.exists", return_value=True):
            file_module = FILE(filename=self.filename)
            file_module.transmit(topic, data)

            mock_open_func.assert_any_call(self.filename, "r")
            mock_open_func.assert_any_call(self.filename, "w")

            handle = mock_open_func()
            written_data = "".join(call.args[0] for call in handle.write.call_args_list)
            expected_data = json.dumps(
                {"existing_topic": ["string_value", "new_data"]}, indent=4
            )
            self.assertEqual(written_data, expected_data)

    def test_transmit_adds_new_topic(self):
        topic = "new_topic"
        data = "test_data"

        with patch("builtins.open", mock_open(read_data="{}")) as mock_open_func, \
             patch("os.path.exists", return_value=True):
            file_module = FILE(filename=self.filename)
            file_module.transmit(topic, data)

            mock_open_func.assert_any_call(self.filename, "r")
            mock_open_func.assert_any_call(self.filename, "w")

            handle = mock_open_func()
            written_data = "".join(call.args[0] for call in handle.write.call_args_list)
            updated_data = json.dumps({"new_topic": ["test_data"]}, indent=4)
            self.assertEqual(written_data, updated_data)


if __name__ == "__main__":
    unittest.main()
