import sys
import os
import unittest
from unittest.mock import mock_open, patch
import json

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from core.modules.output_modules.file import FILE


class TestFILEOutputModule(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=False)
    def test_transmit_creates_file_with_new_topic(self, mock_exists, mock_open_func):
        file_module = FILE(filename="test.json")
        topic = "test_topic"
        data = "test_data"
        file_module.transmit(topic, data)
        mock_open_func.assert_called_once_with("test.json", "w")
        handle = mock_open_func()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        expected_data = json.dumps({topic: [data]}, indent=4)
        self.assertEqual(written_data, expected_data)

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"existing_topic": ["existing_data"]}',
    )
    @patch("os.path.exists", return_value=True)
    def test_transmit_appends_data_to_existing_topic(self, mock_exists, mock_open_func):
        file_module = FILE(filename="test.json")
        topic = "existing_topic"
        data = "new_data"
        file_module.transmit(topic, data)
        mock_open_func.assert_any_call("test.json", "r")
        mock_open_func.assert_any_call("test.json", "w")
        handle = mock_open_func()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        expected_data = json.dumps(
            {"existing_topic": ["existing_data", "new_data"]}, indent=4
        )
        self.assertEqual(written_data, expected_data)

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"existing_topic": "string_value"}',
    )
    @patch("os.path.exists", return_value=True)
    def test_transmit_converts_existing_non_list_to_list(
        self, mock_exists, mock_open_func
    ):
        file_module = FILE(filename="test.json")
        topic = "existing_topic"
        data = "new_data"
        file_module.transmit(topic, data)
        mock_open_func.assert_any_call("test.json", "r")
        mock_open_func.assert_any_call("test.json", "w")
        handle = mock_open_func()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        expected_data = json.dumps(
            {"existing_topic": ["string_value", "new_data"]}, indent=4
        )
        self.assertEqual(written_data, expected_data)

    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    @patch("os.path.exists", return_value=True)
    def test_transmit_adds_new_topic(self, mock_exists, mock_open_func):
        file_module = FILE(filename="test.json")
        topic = "new_topic"
        data = "test_data"
        file_module.transmit(topic, data)
        mock_open_func.assert_any_call("test.json", "r")
        mock_open_func.assert_any_call("test.json", "w")
        handle = mock_open_func()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        updated_data = json.dumps({"new_topic": ["test_data"]}, indent=4)
        self.assertEqual(written_data, updated_data)


if __name__ == "__main__":
    unittest.main()
