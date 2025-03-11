import unittest
import sys
import os
import traceback
sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from unittest.mock import MagicMock
from leaf.error_handler.error_holder import ErrorHolder


class TestErrorHolder(unittest.TestCase):
    def setUp(self):
        # Using the new simplified ErrorHolder with no timeframe/threshold.
        self.error_holder = ErrorHolder(adapter_id="test_adapter")

    def test_add_error_and_retrieve(self):
        # Simulate raising and catching an exception.
        try:
            raise ValueError("Test exception")
        except ValueError as exc:
            self.error_holder.add_error(exc)

        errors = self.error_holder.get_unseen_errors()
        self.assertEqual(len(errors), 1, "One error should be retrieved")
        # errors[0] is a tuple (exception_obj, traceback_str)
        exception_obj, traceback_str = errors[0]
        self.assertIsInstance(exception_obj, ValueError)
        self.assertIn("ValueError: Test exception", traceback_str,
                      "Traceback should contain the original exception info")

        # Ensure that subsequent calls return no errors (they were cleared).
        errors_after_clearing = self.error_holder.get_unseen_errors()
        self.assertEqual(len(errors_after_clearing), 0,
                         "No errors should remain after clearing")


if __name__ == "__main__":
    unittest.main()