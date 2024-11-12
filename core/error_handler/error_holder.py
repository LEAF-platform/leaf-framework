import threading
import traceback
import logging
import time
from collections import defaultdict
from typing import Optional, List, Dict, Tuple, Any
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class ErrorHolder:
    """
    Class for managing and tracking errors with functionality 
    to capture severity of error and 
    control frequency-based escalation.

    This class tracks errors with timestamps, severity levels, 
    and tracebacks, and it provides a mechanism to escalate error 
    severity if the same error occurs multiple times within a 
    specified timeframe.
    """

    def __init__(self, adapter_id: Optional[str] = None, 
                 timeframe: int = 60, threshold: int = 3):
        """
        Initialise the ErrorHolder instance.

        Args:
            adapter_id (Optional[str]): Identifier for the adapter 
                                        using this error holder.
            timeframe (int): Time in seconds to track errors for 
                             frequency-based severity escalation.
            threshold (int): Number of occurrences of the same 
                             error needed to trigger severity escalation.
        """
        self._errors: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self._adapter_id = adapter_id
        self.timeframe = timeframe
        self.threshold = threshold
        self.error_tracker: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def add_error(self, exc: Exception) -> None:
        """
        Add an error to the holder with tracking for 
        escalation if frequency exceeds threshold.

        Args:
            exc (Exception): The exception to add. The exception 
                              must have a 'severity' attribute.

        Raises:
            AttributeError: If the exception does not 
                            have a 'severity' attribute.
        """
        with self.lock:
            tb = traceback.format_exc()
            if not hasattr(exc, "severity"):
                raise AttributeError("Exception is missing a severity.")
            error_message = str(exc)
            recent_errors = self.error_tracker[error_message]

            # Filter out old errors to keep only
            # those within the specified timeframe
            recent_errors = [
                err
                for err in recent_errors
                if time.time() - err["timestamp"] < self.timeframe
            ]

            error_entry = {
                "error": exc,
                "traceback": tb,
                "timestamp": time.time(),
                "count": 1,
                "is_seen": False,
            }
            recent_errors.append(error_entry)
            self.error_tracker[error_message] = recent_errors

            similar_error_count = len(recent_errors)
            # Escalate severity and reset tracking if error 
            # frequency exceeds threshold
            if similar_error_count >= self.threshold:
                exc.upgrade_severity()
                recent_errors.clear()

            # Append to the main error log
            self._errors.append(error_entry)

    def get_unseen_errors(self) -> List[Tuple[Exception, str]]:
        """
        Retrieve and consolidate all unique unseen errors.

        Marks all instances of each unique error as seen once retrieved.

        Returns:
            List[Tuple[Exception, str]]: A list of tuples containing the
                                        exception and its traceback
                                        for each unique unseen error.
        """
        unseen_errors = {}
        with self.lock:
            for error_entry in self._errors:
                error_message = str(error_entry["error"])
                if not error_entry["is_seen"]:
                    if error_message not in unseen_errors:
                        # Store the first occurrence of each unique error
                        unseen_errors[error_message] = (
                            error_entry["error"],
                            error_entry["traceback"],
                        )
                    error_entry["is_seen"] = True

        return list(unseen_errors.values())

    def cleanup_old_errors(self) -> None:
        """
        Remove errors that are older than the 
        specified timeframe.

        This ensures that error accumulated over a large 
        period of time dont cause a superflous error.
        """
        with self.lock:
            current_time = time.time()
            self._errors = [
                err
                for err in self._errors
                if current_time - err["timestamp"] < self.timeframe
            ]
