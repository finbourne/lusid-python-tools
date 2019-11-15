import os
import unittest
from parameterized import parameterized
from pathlib import Path
import pandas as pd
from lusidtools.logger import LusidLogger
from lusidtools.cocoon import (
    load_data_to_df_and_detect_delimiter,
    check_mapping_fields_exist,
    parse_args,
    identify_cash_items,
    get_delimiter,
)


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
                        "header:{}_footer:{}_delimiter:{}".format(
                            num_h,
                            num_f,
                            str(os.path.basename(delimiter_file_names[count])),
                        ),
                        {
                            "file_path": delimiter_file_names[count],
                            "num_header": num_h,
                            "num_footer": num_f,
                            "delimiter": delim,
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
        print("")

    @parameterized.expand(
        [
            ("comma", ","),
            ("vertical bar", "|"),
            ("percent", "%"),
            ("ampersand", "&"),
            ("backslash", "/"),
            ("tilde", "~"),
            ("asterisk", "*"),
            ("hash", "#"),
            ("tab", "{}".format("\t")),
        ]
    )
    def test_get_delimiter(self, _, delimiter):
        sample_string = [f"data{i}" + delimiter for i in range(10)]
        sample_string = "".join(sample_string)
        delimiter_detected = get_delimiter(sample_string)
        self.assertEqual(delimiter, delimiter_detected)

    def test_check_mapping_fields_exist(self):
        required_list = ["field1", "field4", "field6"]
        search_list = ["field1", "field2", "field3", "field4", "field5", "field6"]
        self.assertFalse(
            check_mapping_fields_exist(required_list, search_list, "test_file_type")
        )

    def test_check_mapping_fields_exist_fail(self):
        LusidLogger2 = LusidLogger("debug")
        required_list = ["field1", "field4", "field7", "field8"]
        search_list = ["field1", "field2", "field3", "field4", "field5", "field6"]

        with self.assertRaises(ValueError):
            check_mapping_fields_exist(required_list, search_list, "test_file_type")

    @parameterized.expand(
        [
            (
                "only_required",
                ["-f", valid_args["file_path"], "-m", valid_args["mapping"],],
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
                ["-f", valid_args["file_path"], "-m", valid_args["mapping"], "-dr",],
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
    def test_parse_args(self, _, args_true, ground_truth):
        args_test = parse_args(args_true)

        map_args_to_ground_truth = {
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
                args_test[0][map_args_to_ground_truth[key]], ground_truth[key]
            )
            for key in ground_truth.keys()
        ]

    @parameterized.expand(
        [
            (
                "implicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP"],
            ),
            (
                "explicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                False,
                ["GBP_EXP", "GBP_EXP", "USD_EXP", "USD_EXP"],
            ),
            (
                "combined_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "",
                        },
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_EXP", "GBP_EXP", "USD_EXP", "USD_IMP"],
            ),
            (
                "implicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                True,
                [],
            ),
            (
                "explicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                True,
                [],
            ),
            (
                "combined_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    },
                    "implicit": "internal_currency",
                },
                True,
                [],
            ),
        ]
    )
    def test_identify_cash_items(self, _, cash_flag, remove_cash_items, ground_truth):
        data = {
            "instrument_name": ["inst1", "inst2", "inst3", "inst4", "inst5"],
            "internal_currency": ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP", "APPLUK"],
            "Figi": ["BBG01", None, None, None, "BBG02"],
        }
        identifier_mapping = {"Figi": "figi"}
        ground_truth.append(None)
        mappings = {"identifier_mapping": identifier_mapping, "cash_flag": cash_flag}
        mappings_ground_truth = {
            "identifier_mapping": {"Figi": "figi"},
            "cash_flag": cash_flag,
        }
        if not remove_cash_items:
            mappings_ground_truth["identifier_mapping"][
                "Currency"
            ] = "currency_identifier_for_LUSID"

        dataframe = pd.DataFrame(data)

        dataframe, mappings_test = identify_cash_items(
            dataframe, mappings, remove_cash_items
        )

        self.assertEqual(ground_truth, list(dataframe["currency_identifier_for_LUSID"]))
        self.assertEqual(mappings_ground_truth, mappings_test)

    @parameterized.expand(
        [
            (
                "implicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_EXP", "GBP_EXP", "USD_EXP", "USD_EXP"],
            ),
            (
                "explicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                False,
                ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP"],
            ),
            (
                "combined_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "",
                        },
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_EXP"],
            ),
            (
                "implicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                False,
                [],
            ),
            (
                "explicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                False,
                [],
            ),
            (
                "combined_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    },
                    "implicit": "internal_currency",
                },
                False,
                [],
            ),
        ]
    )
    def test_identify_cash_items_failed(
        self, _, cash_flag, remove_cash_items, ground_truth
    ):
        data = {
            "instrument_name": ["inst1", "inst2", "inst3", "inst4", "inst5"],
            "internal_currency": ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP", "APPLUK"],
            "Figi": ["BBG01", None, None, None, "BBG02"],
        }
        identifier_mapping = {"Figi": "figi"}
        ground_truth.append(None)
        mappings = {"identifier_mapping": identifier_mapping, "cash_flag": cash_flag}
        mappings_ground_truth = {
            "identifier_mapping": {"Figi": "figi"},
            "cash_flag": cash_flag,
        }
        if not remove_cash_items:
            mappings_ground_truth["identifier_mapping"][
                "Currency"
            ] = "currency_identifier_for_LUSID"

        dataframe = pd.DataFrame(data)

        dataframe, mappings_test = identify_cash_items(
            dataframe, mappings, remove_cash_items
        )

        with self.assertRaises(AssertionError):
            if remove_cash_items:
                self.assertEqual(
                    1, len(list(dataframe["currency_identifier_for_LUSID"]))
                )
            else:
                self.assertEqual(
                    4, len(list(dataframe["currency_identifier_for_LUSID"]))
                )
            self.assertEqual(
                ground_truth, list(dataframe["currency_identifier_for_LUSID"])
            )
            self.assertEqual(mappings_ground_truth, mappings_test)
