import os
import unittest
from parameterized import parameterized
from pathlib import Path
from lusidtools.logger import LusidLogger
from lusidtools.cocoon import load_data_to_df_and_detect_delimiter, parse_args


class AppTests(unittest.TestCase):
    test_data_root = Path(__file__).parent.joinpath("test_data")
    mapping_valid = test_data_root.joinpath("mapping.json")
    mapping_invalid = test_data_root.joinpath("mapping_invalid.json")
    valid_instruments = test_data_root.joinpath("instruments.csv")
    cur_dir = os.path.dirname(__file__)
    secrets = str(Path(__file__).parent.parent.joinpath("secrets.json"))
    testscope = "test-scope"

    valid_args = {
        "file_path": os.path.join(cur_dir, valid_instruments),
        "secrets_file": secrets,
        "mapping": os.path.join(cur_dir, mapping_valid),
        "delimiter": None,
        "scope": testscope,
        "num_header": 0,
        "num_footer": 0,
        "scale_quotes": True,
        "debug": "debug",
        "dryrun": None,
        "line_terminator": r"\n",
    }

    num_header = [0, 2]
    num_footer = [0, 2]
    delimiter_file_names = [
        os.path.join(cur_dir, test_data_root.joinpath("instruments.csv")),
        os.path.join(cur_dir, test_data_root.joinpath("instruments_tab.csv")),
    ]
    delimiter = [",", "{}".format("\t")]
    args_list = []
    for num_h in num_header:
        for num_f in num_footer:
            count = 0
            for delim in delimiter:
                args_list.append(
                    (
                        f"header:{num_h}_footer:{num_f}_delimiter:{str(os.path.basename(delimiter_file_names[count]))}",
                        {
                            "file_path": delimiter_file_names[count],
                            "num_header": num_h,
                            "num_footer": num_f,
                            "delimiter": delim,
                            "scale_quotes": True,
                            "line_terminator": r"\n",
                        },
                    )
                )
                count += 1

    @classmethod
    def setUpClass(cls) -> None:
        LusidLogger(cls.valid_args["debug"])

    @parameterized.expand(args_list)
    def test_get_instruments_data(self, _, args_list):
        data = load_data_to_df_and_detect_delimiter(args_list)
        if args_list["num_header"] == 0:
            self.assertEqual(data[data.columns[0]][0], "Amazon_Nasdaq_AMZN")
        else:
            self.assertEqual(data[data.columns[0]][0], "BP_LondonStockEx_BP")
        if args_list["num_footer"] == 0:
            self.assertEqual(data.tail(1).values[0][0], "Whitebread_LondonStockEx_WTB")
        else:
            self.assertEqual(data.tail(1).values[0][0], "USTreasury_6.875_2025")

    @parameterized.expand(
        [
            [
                "test_xlsx_file",
                {
                    "file_path": os.path.join(
                        cur_dir, test_data_root.joinpath("instruments.xlsx")
                    ),
                    "num_header": 0,
                    "num_footer": 0,
                    "delimiter": None,
                    "scale_quotes": False,
                    "line_terminator": None,
                },
            ]
        ]
    )
    def test_get_instruments_data_excel(self, _, args_list):
        data = load_data_to_df_and_detect_delimiter(args_list)
        if args_list["num_header"] == 0:
            self.assertEqual(data[data.columns[0]][0], "Amazon_Nasdaq_AMZN")
        else:
            self.assertEqual(data[data.columns[0]][0], "BP_LondonStockEx_BP")
        if args_list["num_footer"] == 0:
            self.assertEqual(data.tail(1).values[0][0], "Whitebread_LondonStockEx_WTB")
        else:
            self.assertEqual(data.tail(1).values[0][0], "USTreasury_6.875_2025")
        print("")

    @parameterized.expand(
        [
            (
                "only_required",
                ["-f", valid_args["file_path"], "-m", valid_args["mapping"]],
                {"-f": valid_args["file_path"], "-m": valid_args["mapping"]},
            ),
            (
                "optional_arg_secrets",
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                    "-c",
                    valid_args["secrets_file"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-c": valid_args["secrets_file"],
                },
            ),
            (
                "optional_arg_scope",
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                    "-s",
                    valid_args["scope"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-s": valid_args["scope"],
                },
            ),
            (
                "optional_arg_delimiter",
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                    "-dl",
                    valid_args["delimiter"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-dl": valid_args["delimiter"],
                },
            ),
            (
                "optional_arg_num_header",
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                    "-nh",
                    valid_args["num_header"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-nh": valid_args["num_header"],
                },
            ),
            (
                "optional_arg_num_footer",
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                    "-nf",
                    valid_args["num_footer"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-nf": valid_args["num_footer"],
                },
            ),
            (
                "optional_arg_dryrun",
                ["-f", valid_args["file_path"], "-m", valid_args["mapping"],],
                {"-f": valid_args["file_path"], "-m": valid_args["mapping"],},
            ),
            (
                "optional_arg_dryrun",
                ["-f", valid_args["file_path"], "-m", valid_args["mapping"], "-dr"],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-dr": True,
                },
            ),
            (
                "optional_debug",
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                    "-d",
                    valid_args["debug"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                    "-d": valid_args["debug"],
                },
            ),
        ]
    )
    def test_parse_args(self, _, args_true, expected_value):
        args_test = parse_args(args_true)

        map_args_to_expected_value = {
            "-f": "file_path",
            "-m": "mapping",
            "-c": "secrets_file",
            "-s": "scope",
            "-dl": "delimiter",
            "-nh": "num_header",
            "-nf": "num_footer",
            "-lt": "line_terminator",
            "-b": "batch_size",
            "-dr": "dryrun",
            "-d": "debug",
        }

        [
            self.assertEqual(
                args_test[0][map_args_to_expected_value[key]], expected_value[key]
            )
            for key in expected_value.keys()
        ]
