import os
import sys
import unittest
import itertools
import requests
from unittest.mock import patch, MagicMock
from requests.models import Response
import time

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from core.modules.input_modules.api_watcher import APIWatcher
from core.metadata_manager.metadata import MetadataManager


class TestAPIWatcher(unittest.TestCase):
    def setUp(self):
        self.metadata_manager = MagicMock(spec=MetadataManager)
        self.measurement_url = "http://api.example.com/measurement"
        self.start_url = "http://api.example.com/start"
        self.stop_url = "http://api.example.com/stop"
        self.interval = 60

    def test_with_running_callback(self):
        """Test that the measurement callback is triggered when data changes."""

        class MockResponse:
            def __init__(self, status_code, json_data, headers):
                self.status_code = status_code
                self._json_data = json_data
                self.headers = headers

            def json(self):
                return self._json_data

            def raise_for_status(self):
                """
                Mimic requests.Response.raise_for_status().
                Raises an HTTPError if the status code is not 200-299.
                """
                if not (200 <= self.status_code < 300):
                    raise requests.HTTPError(f"{self.status_code} Error")

        callback_called = {"count": 0}

        def measurement_callback(data):
            callback_called["count"] += 1

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=1,
        )
        with patch("requests.get") as mock_get:
            mock_response_1 = MockResponse(
                status_code=200,
                json_data={"data": "initial_value"},
                headers={"ETag": "12345"},
            )
            mock_response_2 = MockResponse(
                status_code=200,
                json_data={"data": "new_value"},
                headers={"ETag": "67890"},
            )
            mock_get.side_effect = itertools.cycle([mock_response_1, mock_response_2])
            watcher.start()
            time.sleep(2)
            watcher.stop()
        self.assertEqual(callback_called["count"], 2)

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_measurement_polling_etag_unchanged(self, mock_get):
        """Test that the measurement URL does
        not trigger callback if ETag is unchanged."""
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"ETag": "12345"}
        mock_response.json.return_value = {"data": "old_value"}
        mock_get.return_value = mock_response
        measurement_callback = MagicMock()
        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
        )
        watcher._poll_url("measurement", watcher._measurement_callbacks)
        measurement_callback.assert_called_once_with({"data": "old_value"})

        self.assertEqual(watcher.url_states["measurement"].etag, "12345")
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "old_value"}
        )

        mock_response.json.return_value = {"data": "old_value"}
        watcher._poll_url("measurement", watcher._measurement_callbacks)

        self.assertEqual(measurement_callback.call_count, 1)

        self.assertEqual(watcher.url_states["measurement"].etag, "12345")
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "old_value"}
        )

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_measurement_polling_etag_changed(self, mock_get):
        """Test that the measurement URL
        triggers callback if ETag is changed."""

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"ETag": "12345"}
        mock_response.json.return_value = {"data": "old_value"}
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
        )

        watcher._poll_url("measurement", watcher._measurement_callbacks)

        measurement_callback.assert_called_once_with({"data": "old_value"})

        self.assertEqual(watcher.url_states["measurement"].etag, "12345")
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "old_value"}
        )

        mock_response.headers = {"ETag": "67890"}
        mock_response.json.return_value = {"data": "new_value"}
        watcher._poll_url("measurement", watcher._measurement_callbacks)

        self.assertEqual(measurement_callback.call_count, 2)
        measurement_callback.assert_called_with({"data": "new_value"})

        self.assertEqual(watcher.url_states["measurement"].etag, "67890")
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "new_value"}
        )

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_measurement_polling_last_modified_changed(self, mock_get):
        """Test that the measurement URL
        triggers callback if Last-Modified changes."""

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"Last-Modified": "Wed, 21 Oct 2023 07:28:00 GMT"}
        mock_response.json.return_value = {"data": "old_value"}
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
        )

        watcher._poll_url("measurement", watcher._measurement_callbacks)

        measurement_callback.assert_called_once_with({"data": "old_value"})

        self.assertEqual(
            watcher.url_states["measurement"].last_modified,
            "Wed, 21 Oct 2023 07:28:00 GMT",
        )
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "old_value"}
        )

        mock_response.headers = {"Last-Modified": "Thu, 22 Oct 2023 07:28:00 GMT"}
        mock_response.json.return_value = {"data": "new_value"}
        watcher._poll_url("measurement", watcher._measurement_callbacks)

        self.assertEqual(measurement_callback.call_count, 2)
        measurement_callback.assert_called_with({"data": "new_value"})

        self.assertEqual(
            watcher.url_states["measurement"].last_modified,
            "Thu, 22 Oct 2023 07:28:00 GMT",
        )
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "new_value"}
        )

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_measurement_polling_no_etag_or_last_modified(self, mock_get):
        """Test that the measurement URL
        triggers callback if no ETag or Last-Modified is present."""

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {"data": "initial_value"}
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
        )

        watcher._poll_url("measurement", watcher._measurement_callbacks)

        measurement_callback.assert_called_once_with({"data": "initial_value"})

        self.assertIsNone(watcher.url_states["measurement"].etag)
        self.assertIsNone(watcher.url_states["measurement"].last_modified)
        self.assertEqual(
            watcher.url_states["measurement"].previous_data, {"data": "initial_value"}
        )

        mock_response.json.return_value = {"data": "initial_value"}
        watcher._poll_url("measurement", watcher._measurement_callbacks)

        self.assertEqual(measurement_callback.call_count, 1)

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_304_no_change(self, mock_get):
        """Test that the measurement URL does
        not trigger callbacks if no change (304 Not Modified)."""

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 304
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
        )

        watcher._poll_url("measurement", watcher._measurement_callbacks)

        measurement_callback.assert_not_called()

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_measurement_polling_multiple_no_change(self, mock_get):
        """Test that the measurement URL does not
        trigger callbacks over multiple polls if no change occurs."""

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"ETag": "12345"}
        mock_response.json.return_value = {"data": "value"}
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
        )

        watcher._poll_url("measurement", watcher._measurement_callbacks)

        measurement_callback.assert_called_once_with({"data": "value"})

        for _ in range(3):
            watcher._poll_url("measurement", watcher._measurement_callbacks)

        self.assertEqual(measurement_callback.call_count, 1)

    def test_thread_start_stop(self):
        """Test that the start and stop
        methods manage the thread correctly."""

        with patch.object(APIWatcher, "_poll_url"), patch(
            "time.sleep", return_value=None
        ):
            watcher = APIWatcher(
                self.metadata_manager,
                measurement_url=self.measurement_url,
                interval=self.interval,
            )

            self.assertFalse(watcher.running)

            watcher.start()

            self.assertTrue(watcher.running)
            self.assertIsNotNone(watcher._thread)
            self.assertTrue(watcher._thread.is_alive())

            watcher.stop()

            self.assertFalse(watcher.running)
            self.assertIsNotNone(watcher._thread)
            self.assertFalse(watcher._thread.is_alive())

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_start_stop_polling(self, mock_get):
        """Test that start and stop URLs are polled
        correctly and trigger callbacks on change."""

        mock_start_response = MagicMock(spec=Response)
        mock_start_response.status_code = 200
        mock_start_response.headers = {"ETag": "start_etag"}
        mock_start_response.json.return_value = {"start_data": "start"}

        mock_stop_response = MagicMock(spec=Response)
        mock_stop_response.status_code = 200
        mock_stop_response.headers = {"ETag": "stop_etag"}
        mock_stop_response.json.return_value = {"stop_data": "stop"}

        mock_get.side_effect = [mock_start_response, mock_stop_response]

        start_callback = MagicMock()
        stop_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            start_url=self.start_url,
            stop_url=self.stop_url,
            start_callbacks=start_callback,
            stop_callbacks=stop_callback,
            interval=self.interval,
        )

        watcher._poll_url("start", watcher._start_callbacks)
        watcher._poll_url("stop", watcher._stop_callbacks)

        start_callback.assert_called_once_with({"start_data": "start"})
        stop_callback.assert_called_once_with({"stop_data": "stop"})

        self.assertEqual(watcher.url_states["start"].etag, "start_etag")
        self.assertEqual(watcher.url_states["stop"].etag, "stop_etag")

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_custom_headers_in_request(self, mock_get):
        """Test that custom headers are included in the request."""
        custom_headers = {
            "Authorization": "Bearer test_token",
            "X-Custom-Header": "custom_value",
        }

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"ETag": "12345"}
        mock_response.json.return_value = {"data": "value"}
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
            headers=custom_headers,
        )

        watcher._poll_url("measurement", 
                          watcher._measurement_callbacks)

        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        headers_sent = kwargs["headers"]

        self.assertEqual(headers_sent.get("Authorization"), "Bearer test_token")
        self.assertEqual(headers_sent.get("X-Custom-Header"), "custom_value")

    @patch("core.modules.input_modules.api_watcher.requests.get")
    def test_custom_headers_with_conditional_headers(self, mock_get):
        """Test that custom headers are correctly merged with ETag/Last-Modified headers."""
        custom_headers = {"Authorization": "Bearer test_token"}

        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"ETag": "12345"}
        mock_response.json.return_value = {"data": "initial_value"}
        mock_get.return_value = mock_response

        measurement_callback = MagicMock()

        watcher = APIWatcher(
            self.metadata_manager,
            measurement_url=self.measurement_url,
            measurement_callbacks=measurement_callback,
            interval=self.interval,
            headers=custom_headers,
        )

        watcher._poll_url("measurement", watcher._measurement_callbacks)
        self.assertEqual(watcher.url_states["measurement"].etag, "12345")

        mock_response.headers = {"ETag": "12345"}
        mock_response.json.return_value = {"data": "initial_value"}
        watcher._poll_url("measurement", watcher._measurement_callbacks)

        mock_get.assert_called()
        _, kwargs = mock_get.call_args
        headers_sent = kwargs["headers"]

        self.assertEqual(headers_sent.get("Authorization"), "Bearer test_token")
        self.assertEqual(headers_sent.get("If-None-Match"), "12345")


if __name__ == "__main__":
    unittest.main()
