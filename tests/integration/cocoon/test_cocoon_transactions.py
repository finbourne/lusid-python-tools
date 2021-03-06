import unittest
from pathlib import Path
import pandas as pd
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id


class CocoonTestsTransactions(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature("T8-1", "T8-2", "T8-3", "T8-4", "T8-5", "T8-6")
    @parameterized.expand(
        [
            [
                "Test standard transaction load",
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "operations001",
                None,
                lusid.models.Version,
            ],
            [
                "Test standard transaction load with nulls for optional parameters",
                "prime_broker_test",
                "data/global-fund-combined-transactions-with-nulls.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trans_currency"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "operations001",
                None,
                lusid.models.Version,
            ],
            [
                "Add in some constants",
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                "operations001",
                None,
                lusid.models.Version,
            ],
            [
                "Pass in None for some of properties_scope which accepts this",
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "portfolio_code",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                None,
                None,
                lusid.models.Version,
            ],
            [
                "Pass a constant for the portfolio code",
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "$GlobalCreditFund",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                None,
                None,
                lusid.models.Version,
            ],
            [
                "Try with a small batch",
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "$GlobalCreditFund",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                None,
                2,
                lusid.models.Version,
            ],
            [
                "Try with a small batch & random scope to ensure property cretaion",
                "prime_broker_test",
                "data/global-fund-combined-transactions.csv",
                {
                    "code": "$GlobalCreditFund",
                    "transaction_id": "id",
                    "type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "transaction_price.type": "price_type",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                {"transaction_currency": "trade_currency", "source": "$default"},
                {
                    "Isin": "isin",
                    "Figi": "figi",
                    "ClientInternal": "client_internal",
                    "Currency": "currency_transaction",
                },
                ["exposure_counterparty", "compls", "val", "location_region"],
                f"prime_broker_test_{create_scope_id()}",
                2,
                lusid.models.Version,
            ],
        ]
    )
    def test_load_from_data_frame_transactions_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        batch_size,
        expected_outcome,
    ) -> None:
        """
        Test that transactions

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list[str] property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param any expected_outcome: The expected outcome

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            batch_size=batch_size,
        )

        self.assertGreater(len(responses["transactions"]["success"]), 0)

        self.assertEqual(len(responses["transactions"]["errors"]), 0)

        self.assertTrue(
            expr=all(
                isinstance(success_response.version, expected_outcome)
                for success_response in responses["transactions"]["success"]
            )
        )
