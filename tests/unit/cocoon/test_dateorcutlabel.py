import unittest

import numpy as np
import pandas as pd

from lusidtools import logger
from lusidtools.cocoon.dateorcutlabel import DateOrCutLabel
from parameterized import parameterized
from datetime import datetime
import pytz


class CocoonDateOrCutLabelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            [
                "Already in ISO format",
                "2019-11-04T13:25:34+00:00",
                "2019-11-04T13:25:34+00:00",
            ],
            [
                "Already in ISO format UTC",
                "2019-11-04T13:25:34Z",
                "2019-11-04T13:25:34Z",
            ],
            ["A date with year first", "2019-11-04", "2019-11-04T00:00:00+00:00"],
            ["A date with day first", "04-11-2019", "2019-11-04T00:00:00+00:00"],
            ["A date with month first", "11-04-2019", "2019-11-04T00:00:00+00:00"],
            ["A cut label", "2019-11-04NNYSEClose", "2019-11-04NNYSEClose"],
            [
                "Datetime object with no timezone info",
                datetime(year=2019, month=8, day=5, tzinfo=None),
                "2019-08-05T00:00:00+00:00",
            ],
            [
                "Datetime object with a timezone other than UTC",
                pytz.timezone("America/New_York").localize(
                    datetime(year=2019, month=8, day=5, hour=10, minute=30)
                ),
                "2019-08-05T14:30:00+00:00",
            ],
            [
                "ISO format with no timezone info",
                "2019-04-11T00:00:00",
                "2019-04-11T00:00:00+00:00",
            ],
            [
                "ISO format with no timezone info and has milliseconds specified",
                "2019-11-20T00:00:00.000000000",
                "2019-11-20T00:00:00.000000000+00:00",
            ],
            [
                "Already in ISO format with mircoseconds",
                "2019-09-01T09:31:22.664000+00:00",
                "2019-09-01T09:31:22.664000+00:00",
            ],
            [
                "Already in ISO format with mircoseconds Z timezone",
                "2019-09-01T09:31:22.664000Z",
                "2019-09-01T09:31:22.664000Z",
            ],
            [
                "numpy datetime with nanoseconds",
                np.array(['2019-09-01T09:31:22.664'], dtype='datetime64[ns]'),
                "2019-09-01T09:31:22.664000Z",
            ],            [
                "pandas datetime with nanoseconds",
                pd.Timestamp('2019-09-01T09:31:22.664'),
                "2019-09-01T09:31:22.664000+00:00",
            ],
        ]
    )
    def test_dateorcutlabel(self, test_name, datetime_value, expected_outcome):
        ignore = ["A date with month first"]
        if test_name in ignore:
            self.skipTest("Test not implemented ")
        date_or_cut_label = DateOrCutLabel(datetime_value)

        self.assertEqual(first=date_or_cut_label, second=expected_outcome)
