import unittest

import lusidtools.lpt.dfq as dfq


class DfqTests(unittest.TestCase):
    def test_not_df(self):
        args = dfq.parse(False, ["-g", "Name"])
        result = dfq.dfq(args, "No reconciliation breaks")
        self.assertEqual("No reconciliation breaks", result)
