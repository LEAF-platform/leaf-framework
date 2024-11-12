import logging
from enum import Enum
from core.modules.logger_modules.logger_utils import get_logger
logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)


class SeverityLevel(Enum):
    INFO = 1        # Informational, log only
    WARNING = 2     # Warning, may need attention
    ERROR = 3       # Recoverable error, retry or fallback
    CRITICAL = 4    # Critical, may require immediate action or recovery

class LEAFError(Exception):
    """Base class for other exceptions"""
    def __init__(self, message,severity):
        self._severity = severity
        self._message = message
        super().__init__(message)

    @property
    def severity(self):
        """Return the severity of the error."""
        return self._severity
    
    def upgrade_severity(self):
        upgr_sev = self._next_severity_level(self.severity)
        if upgr_sev != self.severity:
            #logger.warning(f"Severity ({self}) - {self.severity} to {upgr_sev}")
            self._severity = upgr_sev

    def _next_severity_level(self, current_severity):
        """
        Calculate the next severity level, capping at CRITICAL.

        Args:
            current_severity (SeverityLevel): The current severity level.

        Returns:
            SeverityLevel: The next severity level.
        """

        next_value = min(current_severity.value + 1, 
                         SeverityLevel.CRITICAL.value)
        return SeverityLevel(next_value)
    
    def __str__(self):
        return f'{self._message} - {self.__class__.__name__} - {self.severity}'

class InputError(LEAFError):
    '''
    Either the hardware is down, or the input mechanism 
    cannot access the information it should be able to.
    '''
    def __init__(self, reason,severity=SeverityLevel.ERROR):
        message = f"Can't access InputData: {reason}."
        super().__init__(message,severity)

class HardwareStalledError(LEAFError):
    '''
    The hardware appears to have stopped transmitting information.
    '''
    def __init__(self, reason,severity=SeverityLevel.WARNING):
        message = f"Hardware may have stalled: {reason}."
        super().__init__(message,severity)

class ClientUnreachableError(LEAFError):
    '''
    The client OR output mechanism can't post information. 
    For example, the MQTT broker can't be transmitted to.
    '''
    def __init__(self, reason,output_module=None,severity=SeverityLevel.WARNING):
        message = f"Cannot connect or reach client: {reason}."
        super().__init__(message,severity)
        self.client = output_module

class AdapterBuildError(LEAFError):
    '''
    An error occurs when the adapter is being built.
    '''
    def __init__(self, reason,severity=SeverityLevel.CRITICAL):
        message = f"Adapter configuring is invalid: {reason}."
        super().__init__(message,severity)

class AdapterLogicError(LEAFError):
    '''
    Logic within how the adapter has been built causes an error.
    '''
    def __init__(self, reason,severity=SeverityLevel.WARNING):
        message = f"How the adapter has been built has caused an error: {reason}."
        super().__init__(message,severity)

class InterpreterError(LEAFError):
    '''
    The adapter interpreter has some faults that 
    cannot be identified without knowledge of the adapter's specifics.
    '''
    def __init__(self, reason,severity=SeverityLevel.INFO):
        message = f"Error within the interpreter: {reason}."
        super().__init__(message,severity)