import asyncio
import unittest

from core.start import main


class TestStart(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # Call the async main function directly
        await main('../core/config.ini')

    async def test_example(self):
        # Sleep for 10 seconds to allow the main function to run
        await asyncio.sleep(10)




    async def asyncTearDown(self):
        # Cleanup code to run after each test
        pass

if __name__ == '__main__':
    unittest.main()
