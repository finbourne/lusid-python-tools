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
                "Already in ISO format positive offset",
                "2019-11-04T13:25:34+00:00",
                "2019-11-04T13:25:34+00:00",
            ],
            [
                "Already in ISO format negative offset",
                "2020-04-29T09:30:00-05:00",
                "2020-04-29T09:30:00-05:00",
            ],
            [
                "Already in ISO format with microseconds",
                "2012-05-21T00:00:00.0000000+00:00",
                "2012-05-21T00:00:00.0000000+00:00",
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
                "numpy datetime with microseconds",
                np.array(["2019-09-01T09:31:22.664"], dtype="datetime64[ns]"),
                "2019-09-01T09:31:22.664000Z",
            ],
            [
                "pandas datetime with microseconds",
                pd.Timestamp("2019-09-01T09:31:22.664"),
                "2019-09-01T09:31:22.664000+00:00",
            ],
            [
                "numpy datetime64 UTC false",
                np.datetime64("2019-07-02", format="%Y%m%d", utc=False),
                "2019-07-02T00:00:00.000000Z",
            ],
        ]
    )
    def test_dateorcutlabel(self, test_name, datetime_value, expected_outcome):
        # There is no handling for month first, it will assume it is day first
        ignore = ["A date with month first"]
        if test_name in ignore:
            self.skipTest(
                "Test not implemented ")

        date_or_cut_label = DateOrCutLabel(datetime_value)

        self.assertEqual(first=expected_outcome, second=str(date_or_cut_label.data))

    @parameterized.expand(
        [
            [
                "YYYY-mm-dd_dashes",
                "2019-09-01",
                "%Y-%m-%d",
                "2019-09-01T00:00:00.000000Z",
            ],
            [
                "dd/mm/YYYY_ forwardslashes",
                "01/09/2019",
                "%d/%m/%Y",
                "2019-09-01T00:00:00.000000Z",
            ],
            [
                "YYYY-mm-dd HH:MM:SS_dashes_and_colons",
                "2019-09-01 6:30:30",
                "%Y-%m-%d %H:%M:%S",
                "2019-09-01T06:30:30.000000Z",
            ],
            [
                "YYYY-mm-dd HH:MM:SS.000001_dashes_colons_and microseconds",
                "2019-09-01 6:30:30.005001",
                "%Y-%m-%d %H:%M:%S.%f",
                "2019-09-01T06:30:30.005001Z",
            ],
        ]
    )
    def test_dateorcutlabel_with_custom_format(
        self, test_name, datetime_value, custom_format, expected_outcome
    ):

        date_or_cut_label = DateOrCutLabel(datetime_value, custom_format)
        self.assertEqual(first=expected_outcome, second=str(date_or_cut_label.data))
