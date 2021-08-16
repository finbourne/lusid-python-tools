import io
import unittest
from unittest.mock import patch

import pandas

import lusidtools.lpt.dfq as dfq
from pandas.testing import assert_frame_equal


class DfqTests(unittest.TestCase):
    @unittest.mock.patch("sys.stdout", new_callable=io.StringIO)
    def test_not_df(self, mock_stdout):
        with self.assertRaises(SystemExit):
            args = dfq.parse(False, ["-g", "Name"])
            dfq.dfq(args, "No reconciliation breaks")
            self.assertEqual("No reconciliation breaks", mock_stdout.getvalue().strip())

    @unittest.mock.patch("sys.stdout", new_callable=io.StringIO)
    def test_first(self, mock_stdout):
        args = dfq.parse(False, ["-f", "2"])
        dfq.dfq(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Foo", "Value": 2},
                    {"Name": "Foo", "Value": 5},
                    {"Name": "Bar", "Value": 7},
                ]
            ),
        )

        expected_df = pandas.DataFrame(
            [{"Name": "Foo", "Value": 2}, {"Name": "Foo", "Value": 5},]
        ).to_string(index=False)
        expected = f"First 2\n{expected_df}"

        self.assertEqual(expected, mock_stdout.getvalue().strip())

    @unittest.mock.patch("sys.stdout", new_callable=io.StringIO)
    def test_last(self, mock_stdout):
        args = dfq.parse(False, ["-l", "1"])
        dfq.dfq(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Foo", "Value": 2},
                    {"Name": "Foo", "Value": 5},
                    {"Name": "Bar", "Value": 7},
                ]
            ),
        )

        expected_df = pandas.DataFrame([{"Name": "Bar", "Value": 7},]).to_string(
            index=False
        )
        expected = f"Last 1\n{expected_df}"

        self.assertEqual(expected, mock_stdout.getvalue().strip())

    @unittest.mock.patch("sys.stdout", new_callable=io.StringIO)
    def test_first_last_both_0(self, mock_stdout):
        args = dfq.parse(False, ["-l", "0", "-f", "0"])
        dfq.dfq(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Foo", "Value": 2},
                    {"Name": "Foo", "Value": 5},
                    {"Name": "Bar", "Value": 7},
                ]
            ),
        )

        expected_df = pandas.DataFrame(
            [
                {"Name": "Foo", "Value": 2},
                {"Name": "Foo", "Value": 5},
                {"Name": "Bar", "Value": 7},
            ]
        ).to_string(index=False)
        expected = f"{expected_df}"

        self.assertEqual(expected, mock_stdout.getvalue().strip())

    def test_group_by(self):
        args = dfq.parse(False, ["-g", "Name"])
        result = dfq.apply_args(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Foo", "Value": 2},
                    {"Name": "Foo", "Value": 5},
                    {"Name": "Bar", "Value": 7},
                ]
            ),
        )

        assert_frame_equal(
            pandas.DataFrame(
                [{"Name": "Bar", "Value": 7}, {"Name": "Foo", "Value": 7},]
            ),
            result,
        )

    @unittest.mock.patch("sys.stdout", new_callable=io.StringIO)
    def test_columns(self, mock_stdout):
        args = dfq.parse(False, ["--columns"])

        with self.assertRaises(SystemExit):
            dfq.dfq(args, pandas.DataFrame([{"Name": "Foo", "Value": 2},]))

            expected = "Name\nValue"

            self.assertEqual(expected, mock_stdout.getvalue().strip())

    def test_unique(self):
        args = dfq.parse(False, ["-u"])
        result = dfq.apply_args(
            args,
            pandas.DataFrame(
                [{"Name": "Foo", "Value": 2}, {"Name": "Foo", "Value": 2}]
            ),
        )

        assert_frame_equal(pandas.DataFrame([{"Name": "Foo", "Value": 2}]), result)

    def test_where(self):
        args = dfq.parse(False, ["--where", "Value>3"])

        result = dfq.apply_args(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Foo", "Value": 2},
                    {"Name": "Bar", "Value": 4},
                    {"Name": "Baz", "Value": 10},
                ]
            ),
        )

        expected_df = pandas.DataFrame(
            [{"Name": "Bar", "Value": 4}, {"Name": "Baz", "Value": 10},]
        )

        assert_frame_equal(
            expected_df.reset_index(drop=True), result.reset_index(drop=True)
        )

    def test_where_multiple_conditions(self):
        args = dfq.parse(False, ["--where", "Value>3", "Name=Baz"])

        result = dfq.apply_args(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Foo", "Value": 2},
                    {"Name": "Bar", "Value": 4},
                    {"Name": "Baz", "Value": 10},
                ]
            ),
        )

        assert_frame_equal(
            pandas.DataFrame([{"Name": "Baz", "Value": 10},]).reset_index(drop=True),
            result.reset_index(drop=True),
        )

    def test_order(self):
        args = dfq.parse(False, ["--order", "Name"])

        result = dfq.apply_args(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Bar", "Value": 2},
                    {"Name": "Foo", "Value": 4},
                    {"Name": "Baz", "Value": 10},
                ]
            ),
        )

        expected_df = pandas.DataFrame(
            [
                {"Name": "Bar", "Value": 2},
                {"Name": "Baz", "Value": 10},
                {"Name": "Foo", "Value": 4},
            ]
        )

        assert_frame_equal(
            expected_df.reset_index(drop=True), result.reset_index(drop=True)
        )

    def test_select(self):
        args = dfq.parse(False, ["--select", "Name", "Value1"])

        result = dfq.apply_args(
            args,
            pandas.DataFrame(
                [
                    {"Name": "Bar", "Value1": 2, "Value2": 4},
                    {"Name": "Foo", "Value1": 4, "Value2": 6},
                    {"Name": "Baz", "Value1": 10, "Value2": 8},
                ]
            ),
        )

        expected_df = pandas.DataFrame(
            [
                {"Name": "Bar", "Value1": 2},
                {"Name": "Foo", "Value1": 4},
                {"Name": "Baz", "Value1": 10},
            ]
        )

        assert_frame_equal(
            expected_df.reset_index(drop=True), result.reset_index(drop=True)
        )
