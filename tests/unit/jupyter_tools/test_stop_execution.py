import unittest
from lusidtools.jupyter_tools import StopExecution


class TestPortfolioStopper(unittest.TestCase):
    def test_portfolio_stopper(self) -> None:
        portfolio_code = "testPortfolio"
        error_message = f"Portfolio {portfolio_code} does not exist. Stopping notebook"

        with self.assertRaises(StopExecution) as error:
            raise StopExecution(error_message)

        self.assertEqual(error.exception.message, error_message)
