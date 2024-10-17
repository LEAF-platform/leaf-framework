from abc import abstractmethod
from abc import ABC

class MeasurementModule(ABC):
    """
    Abstract class that defines the structure for measurement adapters.
    Take in ambiguous representations of specific 
    measurements (e.g., O2, pH) from equipment and return a 
    uniform measurement in specific units using agreed-upon terms.
    """
    
    def __init__(self, term):
        """
        Initialise the MeasurementModule with a
        specific measurement term.

        Args:
            term: The term representing the 
            type of measurement (e.g., O2, pH).
        """
        self._term = term

    @property
    def term(self):
        """
        The measurement term.
        
        Returns:
            The term associated with the measurement (e.g., O2, pH).
        """
        return self._term
    
    @abstractmethod
    def transform(self, measurement):
        """
        Abstract method to transform an 
        ambiguous measurement into a standardised form.

        Args:
            measurement: The raw measurement data to be transformed.

        Returns:
            A standardised measurement based 
            on the defined transformation logic.
        """
        pass
