import logging
import os
import unittest
from parameterized import parameterized
import datetime
from dateutil.tz import tzutc
from pathlib import Path
from lusid import PortfoliosApi, TransactionPortfoliosApi
import lusid
from lusid.utilities import ApiClientBuilder

from tests.unit.apps.test_data import test_transactions as flush_test_data

from lusidtools.apps import (
    load_instruments,
    load_holdings,
    load_transactions,
    load_quotes,
    load_portfolios,
    flush_transactions
)
from lusidtools.cocoon import cocoon_printer
from lusidtools.logger import LusidLogger


class AppTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        test_data_root = Path(__file__).parent.joinpath("test_data")
        cls.mapping_valid = test_data_root.joinpath("mapping.json")
        cls.mapping_invalid = test_data_root.joinpath("mapping_invalid.json")
        cls.valid_instruments = test_data_root.joinpath("instruments.csv")
        cls.cur_dir = os.path.dirname(__file__)
        cls.secrets = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.testscope = "testscope0001"
        cls.proptestscope = "testscope0001"

        factory = lusid.utilities.ApiClientFactory(api_secrets_filename=cls.secrets)
        cls.transaction_portfolios_api = factory.build(TransactionPortfoliosApi)

        cls.valid_args = {
            "file_path": os.path.join(cls.cur_dir, cls.valid_instruments),
            "secrets_file": cls.secrets,
            "mapping": os.path.join(cls.cur_dir, cls.mapping_valid),
            "delimiter": None,
            "scope": cls.testscope,
            "property_scope": cls.proptestscope,
            "num_header": 0,
            "num_footer": 0,
            "debug": "debug",
            "batch_size": None,
            "dryrun": False,
            "line_terminator": r"\n",
            "display_response_head": True,
        }

        cls.invalid_args = {
            "file_path": os.path.join(cls.cur_dir, cls.valid_instruments),
            "secrets_file": cls.secrets,
            "mapping": os.path.join(cls.cur_dir, cls.mapping_invalid),
            "delimiter": None,
            "scope": cls.testscope,
            "num_header": 0,
            "num_footer": 0,
            "debug": "debug",
            "batch_size": None,
            "dryrun": False,
            "line_terminator": r"\n",
            "display_response_head": True,
        }

        LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

        cls.portfolios_api = factory.build(PortfoliosApi)
        portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
            scope=cls.testscope
        )

        existing_portfolios = [
            portfolio.id.code for portfolio in portfolios_response.values
        ]
        test_list = ["Global-Strategies-SHK", "GlobalCreditFund", "TestFlushPortfolio"]

        if not all(x in existing_portfolios for x in test_list):
            transactions_portfolio_api = factory.build(TransactionPortfoliosApi)
            for portfolio in test_list:
                if portfolio not in existing_portfolios:
                    transaction_portfolio_request1 = lusid.models.CreateTransactionPortfolioRequest(
                        display_name=portfolio,
                        code=portfolio,
                        base_currency="GBP",
                        created="2018-03-05T12:00:00+00:00",
                        sub_holding_keys=[f"Transaction/{cls.testscope}/currency"],
                    )
                    transactions_portfolio_response1 = transactions_portfolio_api.create_portfolio(
                        scope=cls.testscope,
                        create_transaction_portfolio_request=transaction_portfolio_request1,
                    )
                    logging.info(f"created portfolio: {portfolio}")

    def test_upsert_portfolios_withvalid_mapping(self):
        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath(
            "holdings.csv"
        )  # details are specified in the mapping as literal values
        args["mapping"] = test_data_root.joinpath("mapping_port.json")

        # Check portfolios loaded correctly
        responses = load_portfolios(args)
        self.assertEqual(1, len(responses["portfolios"]["success"]))

        # delete test portfolio before next test
        factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=args["secrets_file"]
        )
        portfolios_api = factory.build(PortfoliosApi)
        response = portfolios_api.delete_portfolio(
            self.testscope, "temp_upsert_portfolios_lpt"
        )

    def test_upsert_instruments_with_valid_mapping(self):

        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("instruments.csv")
        args["mapping"] = test_data_root.joinpath("mapping_inst.json")

        responses = load_instruments(args)

        self.assertEqual(0, len(responses["instruments"]["errors"]))
        self.assertEqual(
            20,
            sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["success"]
                ]
            ),
        )
        self.assertEqual(
            0,
            sum(
                [
                    len(response.failed)
                    for response in responses["instruments"]["success"]
                ]
            ),
        )

    def test_upsert_instruments_with_invalid_mapping(self):

        args = self.invalid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("instruments.csv")
        args["mapping"] = test_data_root.joinpath("mapping_inst_invalid.json")

        LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))
        responses = load_instruments(args)

        self.assertEqual(0, len(responses["instruments"]["errors"]))
        self.assertEqual(
            20,
            sum(
                [
                    len(response.failed)
                    for response in responses["instruments"]["success"]
                ]
            ),
        )
        self.assertEqual(
            0,
            sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["success"]
                ]
            ),
        )

    def test_upsert_holdings_with_valid_mapping(self):

        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("holdings.csv")
        args["mapping"] = test_data_root.joinpath("mapping_hldgs.json")

        responses = load_holdings(args)

        self.assertEqual(0, len(responses["holdings"]["errors"]))
        self.assertEqual(1, len(responses["holdings"]["success"]))

    def test_upsert_holdings_with_invalid_mapping(self):

        args = self.invalid_args.copy()
        file_type = "holdings"
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("holdings.csv")
        args["mapping"] = test_data_root.joinpath("mapping_hldgs_invalid.json")

        responses = load_holdings(args)

        self.assertEqual(1, len(responses["holdings"]["errors"]))
        self.assertEqual(0, len(responses["holdings"]["success"]))

    def test_upsert_transactions_with_valid_mapping(self):

        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("transactions.csv")
        args["mapping"] = test_data_root.joinpath("mapping_trans.json")

        responses = load_transactions(args)

        self.assertEqual(0, len(responses["transactions"]["errors"]))
        self.assertEqual(1, len(responses["transactions"]["success"]))

    def test_upsert_transactions_with_invalid_mapping(self):

        args = self.invalid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("transactions.csv")
        args["mapping"] = test_data_root.joinpath("mapping_trans_invalid.json")

        responses = load_transactions(args)

        self.assertEqual(1, len(responses["transactions"]["errors"]))
        self.assertEqual(0, len(responses["transactions"]["success"]))

    def test_upsert_quotes_with_valid_mapping(self):

        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("quotes.csv")
        args["mapping"] = test_data_root.joinpath("mapping_quote_valid.json")

        responses = load_quotes(args)

        succ, err, failed = cocoon_printer.format_quotes_response(responses)

        self.assertEqual(0, len(err))
        self.assertEqual(0, len(failed))
        self.assertEqual(14, len(succ))

    def test_upsert_quotes_with_missing_cash_flag(self):

        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("quotes_minimal.csv")
        args["mapping"] = test_data_root.joinpath("mapping_quote_no_cash_flag.json")

        responses = load_quotes(args)

        succ, err, failed = cocoon_printer.format_quotes_response(responses)

        self.assertEqual(0, len(err))
        self.assertEqual(0, len(failed))
        self.assertEqual(6, len(succ))

    def test_upsert_quotes_with_valid_mapping_check_scaled_values(self):

        args = self.valid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("quotes.csv")
        args["mapping"] = test_data_root.joinpath("mapping_quote_valid.json")

        responses = load_quotes(args)

        test_values = [
            [batch.values[value].metric_value.value for value in batch.values]
            for batch in responses["quotes"]["success"]
        ][0]
        expected_value = [
            106.51,
            106.59,
            151.44,
            152.28,
            152.44,
            153.16,
            97.0,
            96.9,
            129.55,
            129.22,
            100.77,
            100.79,
            110.0,
            110.2,
        ]

        self.assertEqual(expected_value, test_values)

    @parameterized.expand([
        [
            "outside_the_test_data_time",
            flush_transactions.parse(args=[
                "testscope0001",
                "TestFlushPortfolio",
                "-s",
                "2020-02-20T00:00:00.0000000+00:00",
                "-e",
                "2020-02-28T23:59:59.0000000+00:00",
            ]),
            6000,
        ],
        [
            "inside_the_test_data_time",
            flush_transactions.parse(args=[
                "testscope0001",
                "TestFlushPortfolio",
                "-s",
                "2020-02-10T00:00:00.0000000+00:00",
                "-e",
                "2020-02-28T23:59:59.0000000+00:00",
            ]),
            0,
        ],
    ])
    def test_flush_between_dates(self, _, args, expected_txn_count):
        # Upsert Test Transaction Data
        self.transaction_portfolios_api.upsert_transactions(self.testscope, "TestFlushPortfolio",
                                                            flush_test_data.gen_transaction_data(6000))

        args.secrets = self.secrets
        flush_transactions.flush(args)

        args.end_date = None
        args.start_date = None
        observed_count = len(flush_transactions.get_all_txns(args))

        self.assertEqual(observed_count, expected_txn_count)

    @classmethod
    def tearDownClass(cls) -> None:
        portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
            scope=cls.testscope
        )

        existing_portfolios = [
            portfolio.id.code for portfolio in portfolios_response.values
        ]

        test_list = ["Global-Strategies-SHK", "GlobalCreditFund", "TestFlushPortfolio"]
        for portfolio in test_list:
            if portfolio in existing_portfolios:
                response = cls.portfolios_api.delete_portfolio(cls.testscope, portfolio)
