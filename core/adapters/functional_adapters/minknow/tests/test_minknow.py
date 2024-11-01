import unittest

from core.adapters.functional_adapters.minknow import interpreter

# Global variables
host = "localhost"
port = 9501
token = 'c50dcbbd-fa64-4f9c-98f7-85c39d98c3c2'

class MINKNOWCase(unittest.TestCase):
    def test_interpreter(self):
        self.assertEqual(True, True)
        minknow = interpreter.MinKNOWInterpreter(host, port, token)
        minknow.measurement("data")



if __name__ == "__main__":
    unittest.main()
