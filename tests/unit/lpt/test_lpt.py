import unittest
from unittest.mock import patch

from parameterized import parameterized

from lusidtools.lpt import lpt


class LptTests(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "Windows absolute path with a sheet",
                "C:\\finbourne\\holdings-examples.xlsx:Multiple",
                "C:\\finbourne\\holdings-examples.xlsx",
                "Multiple",
            ),
            (
                "Unix absolute path with a sheet",
                "/home/finbourne/holdings-examples.xlsx:Multiple",
                "/home/finbourne/holdings-examples.xlsx",
                "Multiple",
            ),
            (
                "Windows absolute path at root with a sheet",
                "C:\\holdings-examples.xlsx:Multiple",
                "C:\\holdings-examples.xlsx",
                "Multiple",
            ),
            (
                "Unix absolute path at root with a sheet",
                "/holdings-examples.xlsx:Multiple",
                "/holdings-examples.xlsx",
                "Multiple",
            ),
            (
                "Windows absolute path with no sheet",
                "C:\\finbourne\\holdings-examples.xlsx",
                "C:\\finbourne\\holdings-examples.xlsx",
                0,
            ),
            (
                "Unix absolute path with no sheet",
                "/holdings-examples.xlsx",
                "/holdings-examples.xlsx",
                0,
            ),
            (
                "Unix path with a colon in the file name with sheet",
                "/home/finbourne/holdings:examples.xlsx:Multiple",
                "/home/finbourne/holdings:examples.xlsx",
                "Multiple",
            ),
            (
                "Unix files with a colon in the file name with no sheet",
                "/home/finbourne/holdings:examples.xlsx",
                "/home/finbourne/holdings:examples.xlsx",
                0,
            ),
            (
                "Windows relative path with a sheet",
                ".\\finbourne\\holdings-examples.xlsx:Multiple",
                ".\\finbourne\\holdings-examples.xlsx",
                "Multiple",
            ),
            (
                "Unix relative path with a sheet",
                "./finbourne/holdings-examples.xlsx:Multiple",
                "./finbourne/holdings-examples.xlsx",
                "Multiple",
            ),
            (
                "Windows relative path with no sheet",
                ".\\finbourne\\holdings-examples.xlsx",
                ".\\finbourne\\holdings-examples.xlsx",
                0,
            ),
            (
                "Unix relative path with no sheet",
                "./finbourne/holdings-examples.xlsx",
                "./finbourne/holdings-examples.xlsx",
                0,
            ),
        ]
    )
    def test_read_input_for_excel(
        self, _, input_path, expected_path, expected_sheet_name
    ):
        """
        Tests that read_input can support both windows and unix specific file paths. In either case need to ensure that
        paths both with and without a suffixed sheet name are correctly parsed and then read as excel files into a
        dataframe.

        :param expected_path: expected path of the excel sheet after the input_path has been parsed
        :param expected_sheet_name: expected sheet of the excel file
        """
        with patch("pandas.read_excel") as read_excel_mock:
            lpt.read_input(path=input_path)
            read_excel_mock.assert_called_once_with(
                expected_path, sheet_name=expected_sheet_name, engine='openpyxl'
            )

    @parameterized.expand(
        [
            ("Windows absolute path", "C:\\finbourne\\holdings-examples.csv"),
            ("Unix absolute path", "/home/finbourne/holdings-examples.csv"),
            ("Windows absolute path at root", "C:\\holdings-examples.txt"),
            ("Unix absolute path at root", "/holdings-examples.txt"),
            ("Windows relative path", ".\\finbourne\\holdings-examples.csv"),
            ("Unix relative path", "./finbourne/holdings-examples.csv"),
            # unlikely or erroneous case of non excel file with sheet
            (
                "Windows non excel file with sheet",
                "C:\\finbourne\\holdings-examples.xyz:ShouldNotHappen",
            ),
            (
                "Unix non excel file with sheet",
                "/home/finbourne/holdings-examples.xyz:ShouldNotHappen",
            ),
        ]
    )
    def test_read_input_for_non_excel(self, _, input_path):
        """
        Tests that non excel file paths do not attempt to retrieve a sheet (which should not be
        viable) and are instead read as csv files into a dataframe.

        """
        with patch("pandas.read_csv") as read_csv_mock:
            lpt.read_input(path=input_path)
            read_csv_mock.assert_called_once_with(input_path)

    @parameterized.expand(
        [
            (
                "Windows absolute path with sheet",
                "C:\\finbourne\\holdings-examples.xlsx:Multiple",
            ),
            (
                "Unix absolute path with sheet",
                "/home/finbourne/holdings-examples.xlsx:Multiple",
            ),
            (
                "Windows absolute path at root with sheet",
                "C:\\holdings-examples.xlsx:Multiple",
            ),
            (
                "Unix absolute path at root with sheet",
                "/holdings-examples.xlsx:Multiple",
            ),
            (
                "Unix allows files with colon in name with sheet",
                "/home/finbourne/holdings:examples.xlsx:Multiple",
            ),
            (
                "Windows path for xls with sheet",
                "C:\\finbourne\\holdings-examples.xls:Multiple",
            ),
            (
                "Unix path for xls with sheet",
                "/home/finbourne/holdings-examples.xls:Multiple",
            ),
            (
                "Windows path for xlsm with sheet",
                "C:\\finbourne\\holdings-examples.xlsm:Multiple",
            ),
            (
                "Unix path for xlsm with sheet",
                "/home/finbourne/holdings-examples.xlsm:Multiple",
            ),
            (
                "Windows path for xlsb with sheet",
                "C:\\finbourne\\holdings-examples.xlsb:Multiple",
            ),
            (
                "Unix path for xlsb with sheet",
                "/home/finbourne/holdings-examples.xlsb:Multiple",
            ),
        ]
    )
    def test_is_path_supported_excel_with_sheet_success(self, _, input_path):
        """
        Tests that all supported excel inputs (xls, xlsx, xlsb, xlsm) with sheets suffixed to the path are matched on
        both unix and windows platforms.

        """
        self.assertTrue(
            lpt.is_path_supported_excel_with_sheet(input_path),
            input_path + " should be a valid excel path with a sheet.",
        )

    @parameterized.expand(
        [
            (
                "Windows absolute path with no sheet",
                "C:\\finbourne\\holdings-examples.xlsx",
            ),
            (
                "Unix absolute path with no sheet",
                "/home/finbourne/holdings-examples.xlsx",
            ),
            ("Windows absolute path at root", "C:\\holdings-examples.xlsx"),
            ("Unix absolute path with no sheet at root", "/holdings-examples.xlsx"),
            (
                "Unix allows files with colon in name with no sheet",
                "/home/finbourne/holdings:examples.xlsx",
            ),
            (
                "Windows path for xlsm with no sheet",
                "C:\\finbourne\\holdings-examples.xlsm",
            ),
            (
                "Unix path for xlsm with no sheet",
                "/home/finbourne/holdings-examples.xlsm",
            ),
            (
                "Windows path for xlsb with no sheet",
                "C:\\finbourne\\holdings-examples.xlsb",
            ),
            (
                "Unix path for xlsb with no sheet",
                "/home/finbourne/holdings-examples.xlsb",
            ),
            ("Non excel file", "/home/finbourne/holdings-examples.xyz"),
            # unlikely or erroneous case of non excel file with sheet
            (
                "Windows non excel file with sheet",
                "C:\\finbourne\\holdings-examples.xyz:ShouldNotHappen",
            ),
            (
                "Unix non excel file with sheet",
                "/home/finbourne/holdings-examples.xyz:ShouldNotHappen",
            ),
        ]
    )
    def test_is_path_supported_excel_with_sheet_failure(self, _, input_path):
        """
        Tests that non supported excel paths or supported excel paths with no sheets suffixed are NOT matched on
        both unix and windows platforms.

        """
        self.assertFalse(
            lpt.is_path_supported_excel_with_sheet(input_path),
            input_path + " should not be a valid excel path with a sheet.",
        )
