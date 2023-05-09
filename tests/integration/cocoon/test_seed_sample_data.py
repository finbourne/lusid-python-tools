import unittest
from pathlib import Path
import pandas as pd
import lusid
from lusidfeature import lusid_feature

from lusidtools.cocoon.seed_sample_data import seed_data
from lusidtools.cocoon.utilities import load_json_file
from lusidtools.cocoon.utilities import create_scope_id
import logging
import datetime
import pytz
import random
from unittest.mock import Mock

logger = logging.getLogger()

secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
seed_sample_data_override_csv = Path(__file__).parent.joinpath(
    "data/seed_sample_data/sample_data_override.csv"
)
sample_data_csv = Path(__file__).parent.joinpath(
    "data/seed_sample_data/sample_data.csv"
)


class CocoonSeedDataTestsBase(object):

    """
    Class description:
    ------------------
    This class creates the tests we use in other classes below.
    We split the file loading and tests into separate classes to improve readability and reuse.
    This class can be extended to your own tests as follows:
    (1) Create a new class with this class as a subclass.
    (2) Load data into LUSID via the setup of new class.
    (3) Run tests against new data.
    """

    @lusid_feature("T12-1")
    def test_transactions_from_response(self):
        transactions_from_response = self.api_factory.build(
            lusid.api.TransactionPortfoliosApi
        ).get_transactions(
            scope=self.scope,
            code=self.sample_data["portfolio_code"].to_list()[0],
            property_keys=[f"Transaction/{self.scope}/strategy"],
        )

        self.assertEqual(
            set([txn.transaction_id for txn in transactions_from_response.values]),
            set(self.sample_data["txn_id"].to_list()),
        )

    @lusid_feature("T12-2")
    def test_portfolio_from_response(self):
        portfolio_from_response = self.api_factory.build(
            lusid.api.PortfoliosApi
        ).get_portfolio(
            scope=self.scope, code=self.sample_data["portfolio_code"].to_list()[0]
        )

        self.assertEqual(
            portfolio_from_response.id.code,
            self.sample_data["portfolio_code"].to_list()[0],
        )

    @lusid_feature("T12-3")
    def test_bad_file_type(self):
        with self.assertRaises(ValueError) as error:
            seed_data(
                self.api_factory,
                ["bad_file_type"],
                self.scope,
                seed_sample_data_override_csv,
                file_type="csv",
            )

        self.assertEqual(
            error.exception.args[0],
            "The provided file_type of bad_file_type has no associated mapping",
        )

    @lusid_feature("T12-4")
    def test_instruments_from_response(self):
        # In this test we take a random sample from the dataframe.
        # We need to make a seperate get_instrument request for each item in the sample, so the test might become
        # inefficient if we take the entire dataframe.
        # A sample of approximately 10 should prove the function works as expected.

        instruments_api = self.api_factory.build(lusid.api.InstrumentsApi)

        random_10_ids = set(
            random.choices(self.sample_data["instrument_id"].to_list(), k=10)
        )
        response_from_random_10 = set(
            [
                response.identifiers["ClientInternal"]
                for response in [
                    instruments_api.get_instrument(
                        identifier_type="ClientInternal", identifier=id
                    )
                    for id in random_10_ids
                ]
            ]
        )

        self.assertEqual(random_10_ids, response_from_random_10)

    @lusid_feature("T12-5")
    def test_sub_holding_keys(self):
        holdings_response = self.api_factory.build(
            lusid.api.TransactionPortfoliosApi
        ).get_holdings(
            scope=self.scope,
            code=self.sample_data["portfolio_code"].to_list()[0],
        )

        list_of_prop_values = [
            holding.sub_holding_keys[f"Transaction/{self.scope}/strategy"].value
            for holding in holdings_response.values
        ]
        unique_strategy_labels = set([prop.label_value for prop in list_of_prop_values])

        self.assertEqual(
            unique_strategy_labels, set(self.sample_data["strategy"].to_list())
        )


class CocoonTestSeedDataNoMappingOverrideCSV(
    unittest.TestCase, CocoonSeedDataTestsBase
):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.sample_data = pd.read_csv(sample_data_csv)

        cls.domain_list = ["portfolios", "instruments", "transactions"]

        cls.seed_data = seed_data(
            cls.api_factory,
            cls.domain_list,
            cls.scope,
            sample_data_csv,
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="csv",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )

    @lusid_feature("T12-6")
    def test_return_dict(self):
        seed_data = self.seed_data

        self.assertEqual(type(seed_data), dict)
        self.assertEqual(set(seed_data.keys()), set(self.domain_list))


class CocoonTestSeedDataWithMappingOverrideCSV(
    unittest.TestCase, CocoonSeedDataTestsBase
):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.sample_data = pd.read_csv(seed_sample_data_override_csv)

    @classmethod
    def tearDownClass(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )

    @lusid_feature("T12-7")
    def test_override_with_custom_mapping(self):
        result = seed_data(
            self.api_factory,
            ["portfolios", "instruments", "transactions"],
            self.scope,
            seed_sample_data_override_csv,
            mappings=dict(
                load_json_file(
                    Path(__file__).parent.parent.parent.joinpath(
                        "integration",
                        "cocoon",
                        "data",
                        "seed_sample_data",
                        "seed_sample_data_override.json",
                    )
                )
            ),
            sub_holding_keys=[f"Transaction/{self.scope}/strategy"],
            file_type="csv",
        )
        self.assertEqual(len(result["portfolios"][0]["portfolios"]["success"]), 1)
        self.assertEqual(len(result["instruments"][0]["instruments"]["success"]), 1)
        self.assertEqual(len(result["transactions"][0]["transactions"]["success"]), 1)


class CocoonTestSeedDataNoMappingOverrideExcel(
    unittest.TestCase, CocoonSeedDataTestsBase
):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.sample_data = pd.read_excel(
            Path(__file__).parent.joinpath(
                "data/seed_sample_data/sample_data_excel.xlsx"
            ),
            engine="openpyxl",
        )

        seed_data(
            cls.api_factory,
            ["portfolios", "instruments", "transactions"],
            cls.scope,
            Path(__file__).parent.joinpath(
                "data/seed_sample_data/sample_data_excel.xlsx"
            ),
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="xlsx",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )


class CocoonTestSeedDataUnsupportedFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = Mock()

    @lusid_feature("T12-8")
    def test_bad_file_type(self):
        with self.assertRaises(ValueError) as error:
            seed_data(
                self.api_factory,
                ["portfolios", "instruments", "transactions"],
                self.scope,
                Path(__file__).parent.joinpath("data/seed_sample_data/sample_data.xml"),
                file_type="xml",
            )

        self.assertEqual(
            error.exception.args[0],
            "Unsupported file type, please upload one of the following: ['csv', 'xlsx']",
        )

    @lusid_feature("T12-9")
    def test_inconsistent_file_extensions(self):
        self.file_extension = "xlsx"

        with self.assertRaises(ValueError) as error:
            seed_data(
                self.api_factory,
                ["portfolios", "instruments", "transactions"],
                self.scope,
                seed_sample_data_override_csv,
                file_type=self.file_extension,
            )

        self.assertEqual(
            error.exception.args[0],
            f"""Inconsistent file and file extensions passed: {seed_sample_data_override_csv} does not have file extension {self.file_extension}""",
        )

    @lusid_feature("T12-10")
    def test_file_not_exist(self):
        self.transaction_file = "data/seed_sample_data/file_not_exist.csv"

        self.assertRaises(
            FileNotFoundError,
            seed_data,
            self.api_factory,
            ["portfolios", "instruments", "transactions"],
            self.scope,
            self.transaction_file,
            file_type="csv",
        )


class CocoonTestSeedDataPassDataFrame(unittest.TestCase, CocoonSeedDataTestsBase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = create_scope_id().replace("-", "_")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.test_dataframe = pd.read_csv(sample_data_csv)
        cls.sample_data = pd.read_csv(sample_data_csv)

        seed_data(
            cls.api_factory,
            ["portfolios", "instruments", "transactions"],
            cls.scope,
            cls.test_dataframe,
            sub_holding_keys=[f"Transaction/{cls.scope}/strategy"],
            file_type="DataFrame",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        # Delete portfolio once tests are concluded
        cls.api_factory.build(lusid.PortfoliosApi).delete_portfolio(
            cls.scope, cls.sample_data["portfolio_code"].to_list()[0]
        )
