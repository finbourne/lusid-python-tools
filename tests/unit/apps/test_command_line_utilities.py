import os
import unittest
from parameterized import parameterized
from pathlib import Path
from lusidtools.logger import LusidLogger
from lusidtools.cocoon import load_data_to_df_and_detect_delimiter, parse_args
from tests.unit.apps.test_data import test_transactions as flush_test_data
import lusidtools.apps.flush_transactions as flush
import lusid
import datetime
from dateutil.tz import tzutc


class AppTests(unittest.TestCase):
    test_data_root = Path(__file__).parent.joinpath("test_data")
    mapping_valid = test_data_root.joinpath("mapping.json")
    mapping_invalid = test_data_root.joinpath("mapping_invalid.json")
    valid_instruments = test_data_root.joinpath("instruments.csv")
    cur_dir = os.path.dirname(__file__)
    secrets = str(Path(__file__).parent.parent.parent.joinpath("secrets.json"))
    testscope = "test-scope"
    code = "FAndFTestPortfolio01"
    api_factory = lusid.utilities.ApiClientFactory(api_secrets_filename=secrets)

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
                            "scale_suotes": True,
                            "line_terminator": r"\n",
                        },
                    )
                )
                count += 1

    @classmethod
    def setUpClass(cls) -> None:
        LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))
        cls.transaction_portfolios_api = cls.api_factory.build(
            lusid.api.TransactionPortfoliosApi
        )
        cls.portfolios_api = cls.api_factory.build(lusid.api.PortfoliosApi)

        portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
            scope=cls.testscope
        )

        existing_portfolios = [
            portfolio.id.code for portfolio in portfolios_response.values
        ]

        for portfolio in existing_portfolios:
            cls.portfolios_api.delete_portfolio(cls.testscope, portfolio)

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
                [
                    "-f",
                    valid_args["file_path"],
                    "-m",
                    valid_args["mapping"],
                ],
                {
                    "-f": valid_args["file_path"],
                    "-m": valid_args["mapping"],
                },
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

    @parameterized.expand(
        [
            [
                "batch-number-greater-than-1",
                4000,
                flush_test_data.gen_transaction_data(
                    1500, datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc())
                ),
                [
                    63,
                    62,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    61,
                    33,
                ],
            ],
            [
                "batch-number-equals-1",
                4000,
                flush_test_data.gen_transaction_data(
                    10, datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc())
                ),
                [10],
            ],
            [
                "larger-max-character-count",
                8000,
                flush_test_data.gen_transaction_data(
                    1500, datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc())
                ),
                [127, 125, 125, 125, 125, 125, 125, 125, 123, 123, 123, 123, 6],
            ],
        ]
    )
    def test_transaction_batcher_by_character_count(
        self, _, maxCharacterCount, whole_txn_set, test_batch_size_list
    ):
        txn_id_lst = [txn["transactionId"] for txn in whole_txn_set]

        batched_data = flush.transaction_batcher_by_character_count(
            "exampleScope",
            "exampleCode",
            "www.exampleHost.lusid.com/api",
            txn_id_lst,
            maxCharacterCount,
        )
        batched_data_size_list = [len(batch) for batch in batched_data]

        self.assertListEqual(batched_data_size_list, test_batch_size_list)
        for batch in batched_data:
            batch_length = sum(len(txn_id) for txn_id in batch) + len(
                f"www.exampleHost.lusid.com/api/api/transactionportfolios/exampleScope/exampleCode/transactions?"
            )
            self.assertLessEqual(batch_length, maxCharacterCount)

    @parameterized.expand(
        [
            [
                "1000-transactions",
                1000,
                flush.parse(
                    args=[
                        "test-scope",
                        "-p",
                        "FAndFTestPortfolio01",
                        "-s",
                        "2020-02-10T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                    ]
                ),
            ],
            [
                "6000-transactions",
                6000,
                flush.parse(
                    args=[
                        "test-scope",
                        "-p",
                        "FAndFTestPortfolio01",
                        "-s",
                        "2020-02-10T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                    ]
                ),
            ],
        ]
    )
    def test_get_all_txns(self, _, txn_num, args):
        self.transaction_portfolios_api.create_portfolio(
            self.testscope,
            {
                "displayName": "TestPortfolio",
                "description": "Portfolio for flush tests",
                "code": self.code,
                "created": "2018-03-05T12:00:00.0000000+00:00",
                "baseCurrency": "USD",
            },
        )
        self.transaction_portfolios_api.upsert_transactions(
            self.testscope,
            self.code,
            flush_test_data.gen_transaction_data(
                txn_num, datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc())
            ),
        )
        args.secrets = self.secrets

        self.assertEqual(txn_num, len(list(flush.get_all_txns(args).values())[0]))

        self.portfolios_api.delete_portfolio(self.testscope, self.code)

    @classmethod
    def tearDownClass(cls) -> None:
        portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
            scope=cls.testscope
        )

        existing_portfolios = [
            portfolio.id.code for portfolio in portfolios_response.values
        ]

        for portfolio in existing_portfolios:
            cls.portfolios_api.delete_portfolio(cls.testscope, portfolio)
