import logging
import os
import unittest
from pathlib import Path
from lusid import PortfoliosApi, TransactionPortfoliosApi
import lusid
from lusid.utilities import ApiClientBuilder
from lusidtools.apps import load_instruments, load_holdings, load_transactions
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

        cls.valid_args = {
            "file_path": os.path.join(cls.cur_dir, cls.valid_instruments),
            "secrets_file": cls.secrets,
            "mapping": os.path.join(cls.cur_dir, cls.mapping_valid),
            "delimiter": None,
            "scope": cls.testscope,
            "num_header": 0,
            "num_footer": 0,
            "debug": "debug",
            "batch_size": None,
            "dryrun": False,
            "line_terminator": r"\n",
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
        }

        LusidLogger(cls.valid_args["debug"])

        factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=cls.valid_args["secrets_file"]
        )
        portfolios_api = factory.build(PortfoliosApi)
        portfolios_response = portfolios_api.list_portfolios_for_scope(
            scope=cls.valid_args["scope"]
        )

        existing_portfolios = [
            portfolio.id.code for portfolio in portfolios_response.values
        ]
        test_list = ["Global-Strategies", "GlobalCreditFund"]

        if not all(x in existing_portfolios for x in test_list):
            transactions_portfolio_api = factory.build(TransactionPortfoliosApi)
            for portfolio in test_list:
                if portfolio not in existing_portfolios:
                    transaction_portfolio_request1 = lusid.models.CreateTransactionPortfolioRequest(
                        display_name=portfolio,
                        code=portfolio,
                        base_currency="GBP",
                        created="2018-03-05T12:00:00+00:00",
                    )
                    transactions_portfolio_response1 = transactions_portfolio_api.create_portfolio(
                        scope=cls.valid_args["scope"],
                        transaction_portfolio=transaction_portfolio_request1,
                    )
                    logging.info(f"created portfolio: {portfolio}")

    def test_upsert_instruments_with_valid_mapping(self):
        test_data_root = Path(__file__).parent.joinpath("test_data")
        self.valid_args["file_path"] = str(test_data_root.joinpath("instruments.csv"))
        responses = load_instruments(self.valid_args)

        self.assertEqual(0, len(responses["instruments"]["errors"]))
        self.assertEqual(
            7,
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
        test_data_root = Path(__file__).parent.joinpath("test_data")
        self.invalid_args["file_path"] = str(test_data_root.joinpath("instruments.csv"))
        LusidLogger(self.invalid_args["debug"])
        responses = load_instruments(self.invalid_args)

        self.assertEqual(0, len(responses["instruments"]["errors"]))
        self.assertEqual(
            7,
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

        LusidLogger(self.valid_args["debug"])

    def test_upsert_holdings_with_valid_mapping(self):
        test_data_root = Path(__file__).parent.joinpath("test_data")
        self.valid_args["file_path"] = str(test_data_root.joinpath("holdings.csv"))
        responses = load_holdings(self.valid_args)

        self.assertEqual(0, len(responses["holdings"]["errors"]))
        self.assertEqual(1, len(responses["holdings"]["success"]))

    def test_upsert_holdings_with_invalid_mapping(self):
        file_type = "holdings"
        test_data_root = Path(__file__).parent.joinpath("test_data")
        self.invalid_args["file_path"] = str(test_data_root.joinpath("holdings.csv"))
        responses = load_holdings(self.invalid_args)

        self.assertEqual(1, len(responses["holdings"]["errors"]))
        self.assertEqual(0, len(responses["holdings"]["success"]))

    def test_upsert_transactions_with_valid_mapping(self):
        test_data_root = Path(__file__).parent.joinpath("test_data")
        self.valid_args["file_path"] = str(test_data_root.joinpath("transactions.csv"))
        responses = load_transactions(self.valid_args)

        self.assertEqual(0, len(responses["transactions"]["errors"]))
        self.assertEqual(1, len(responses["transactions"]["success"]))

    def test_upsert_transactions_with_invalid_mapping(self):
        test_data_root = Path(__file__).parent.joinpath("test_data")
        self.invalid_args["file_path"] = str(test_data_root.joinpath("transactions.csv"))
        responses = load_transactions(self.invalid_args)

        self.assertEqual(1, len(responses["transactions"]["errors"]))
        self.assertEqual(0, len(responses["transactions"]["success"]))
