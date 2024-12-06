import unittest
import time
import sys
import os

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))
from unittest.mock import MagicMock
from leaf.error_handler.exceptions import SeverityLevel
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import LEAFError


class TestErrorHolder(unittest.TestCase):
    def setUp(self):

        self.error_holder = ErrorHolder(
            adapter_id="test_adapter", timeframe=2, threshold=3
        )

    def test_add_error(self):

        mock_error = MagicMock()
        mock_error.severity = SeverityLevel.INFO

        self.error_holder.add_error(mock_error)
        errors = self.error_holder.get_unseen_errors()

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0].severity, SeverityLevel.INFO)

    def test_severity_upgrade_on_frequent_errors(self):
        mock_error = AdapterBuildError("Test Error", severity=SeverityLevel.INFO)
        for _ in range(3):
            self.error_holder.add_error(mock_error)
            time.sleep(0.5)  
        errors = self.error_holder.get_unseen_errors()
        self.assertEqual(errors[0][0].severity, SeverityLevel.WARNING, 
                         "Severity should have been upgraded to WARNING")


    def test_get_unseen_errors_marks_as_seen(self):

        mock_error = MagicMock()
        mock_error.severity = SeverityLevel.WARNING

        self.error_holder.add_error(mock_error)
        unseen_errors = self.error_holder.get_unseen_errors()

        self.assertEqual(len(unseen_errors), 1)

        subsequent_unseen_errors = self.error_holder.get_unseen_errors()
        self.assertEqual(
            len(subsequent_unseen_errors),
            0,
            "There should be no unseen errors after marking as seen",
        )

    def test_cleanup_old_errors(self):

        mock_error1 = MagicMock()
        mock_error1.severity = SeverityLevel.INFO

        self.error_holder.add_error(mock_error1)
        self.error_holder._errors[0]["timestamp"] -= 3
        self.error_holder.cleanup_old_errors()

        errors_after_cleanup = self.error_holder.get_unseen_errors()
        self.assertEqual(
            len(errors_after_cleanup), 0, "Old errors should be cleaned up"
        )

    def test_severity_upgrade_to_critical(self):

        mock_error = LEAFError("MOCK ERROR",severity=SeverityLevel.ERROR)
        self.error_holder.add_error(mock_error)

        for _ in range(5):
            self.error_holder.add_error(mock_error)
            time.sleep(0.5)

        errors = self.error_holder.get_unseen_errors()
        self.assertEqual(
            errors[0][0].severity,
            SeverityLevel.CRITICAL,
            "Severity should be upgraded to CRITICAL",
        )


if __name__ == "__main__":
    unittest.main()
