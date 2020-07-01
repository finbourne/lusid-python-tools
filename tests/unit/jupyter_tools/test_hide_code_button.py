import unittest
from lusidtools.jupyter_tools import toggle_code


class TestHideCodeButton(unittest.TestCase):
    def test_portfolio_stopper(self) -> None:

        test_toggle = toggle_code()
        self.assertEqual(test_toggle, None)
