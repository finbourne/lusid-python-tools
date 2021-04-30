import unittest
from pathlib import Path
import pandas as pd
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id


def transaction_and_instrument_identifiers(trd_0003=False, trd_0004=False):
    if trd_0003:
        trd_0003 = {"trd_0003": {
            "Instrument/default/ClientInternal": "THIS_WILL_NOT_RESOLVE_1",
            "Instrument/default/Sedol": "FAKESEDOL1",
            "Instrument/default/Name": "THIS_WILL_NOT_RESOLVE_1",
        }}
    else:
        trd_0003 = None

    if trd_0004:
        trd_0004 = {"trd_0004": {
            "Instrument/default/ClientInternal": "THIS_WILL_NOT_RESOLVE_2",
            "Instrument/default/Sedol": "FAKESEDOL2",
            "Instrument/default/Name": "THIS_WILL_NOT_RESOLVE_2",
        }}
    else:
        trd_0004 = None

    return [
        transaction for transaction in [trd_0003, trd_0004] if transaction
    ]


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

        # Assert that by default no unmatched_identifiers are returned in the response
        self.assertFalse(responses["transactions"].get("unmatched_identifiers", False))

        self.assertTrue(
            expr=all(
                isinstance(success_response.version, expected_outcome)
                for success_response in responses["transactions"]["success"]
            )
        )

    @parameterized.expand(
        [
            [
                "Test standard transaction load",
                "data/global-fund-combined-transactions.csv",
                True,
                [],
            ],
            [
                "Test standard transaction load with two unresolved instruments",
                "data/global-fund-combined-transactions-unresolved-instruments.csv",
                True,
                [
                    {
                        "unresolved_tx01": {
                            "Instrument/default/ClientInternal": "FAKECLIENTINTERNAL1",
                            "Instrument/default/Figi": "FAKEFIGI1",
                            "Instrument/default/Isin": "FAKEISIN1",
                        }
                    },
                    {
                        "unresolved_tx02": {
                            "Instrument/default/ClientInternal": "FAKECLIENTINTERNAL2",
                            "Instrument/default/Figi": "FAKEFIGI2",
                            "Instrument/default/Isin": "FAKEISIN2",
                        }
                    },
                ],
            ],
        ]
    )
    def test_load_from_data_frame_transactions_success_with_correct_unmatched_identifiers(
        self,
        _,
        file_name,
        return_unmatched_identifiers,
        expected_unmatched_transactions,
    ) -> None:
        """
        Test that transactions are uploaded and have the expected response from load_from_data_frame

        :param str file_name: The name of the test data file
        :param bool return_unmatched_identifiers: A flag to request the return of all unmatched instrument identifiers
        :param expected_unmatched_transactions: The expected unmatched transactions appended to the response

        :return: None
        """
        # Unchanged vars that have no need to be passed via param (they would count as duplicate lines)
        scope = "prime_broker_test"
        mapping_required = {
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
        }
        mapping_optional = {"transaction_currency": "trade_currency"}
        identifier_mapping = {
            "Isin": "isin",
            "Figi": "figi",
            "ClientInternal": "client_internal",
            "Currency": "currency_transaction",
        }
        property_columns = ["exposure_counterparty", "compls", "val", "location_region"]
        properties_scope = "operations001"
        batch_size = None
        expected_outcome = lusid.models.Version

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
            return_unmatched_identifiers=return_unmatched_identifiers,
        )

        self.assertGreater(len(responses["transactions"]["success"]), 0)

        self.assertEqual(len(responses["transactions"]["errors"]), 0)

        # Assert that the unmatched_identifiers returned are as expected for each case
        self.assertEqual(
            responses["transactions"].get("unmatched_identifiers", False),
            expected_unmatched_transactions,
        )

        self.assertTrue(
            expr=all(
                isinstance(success_response.version, expected_outcome)
                for success_response in responses["transactions"]["success"]
            )
        )

    def test_return_unmatched_transactions_extracts_relevant_transactions_and_instruments(
        self,
    ):
        scope = "unmatched_transactions_test"
        code = "MIS_INST_FUND"
        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/transactions_unmatched_instruments.csv"
            )
        )

        # Create a test portfolio
        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "display_name": "portfolio_name",
                "base_currency": "base_currency",
            },
            file_type="portfolio",
            mapping_optional={"created": "txn_trade_date"},
        )

        # Upsert some transactions; one with known id, two with unknown ids
        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "transaction_id": "txn_id",
                "type": "txn_type",
                "transaction_date": "txn_trade_date",
                "settlement_date": "txn_settle_date",
                "units": "txn_units",
                "transaction_price.price": "txn_price",
                "transaction_price.type": "$Price",
                "total_consideration.amount": "txn_consideration",
                "total_consideration.currency": "currency",
            },
            file_type="transaction",
            identifier_mapping={
                "ClientInternal": "instrument_id",
                "Sedol": "sedol",
                "Ticker": "ticker",
                "Name": "name",
            },
            mapping_optional={},
        )

        # Call the return_unmatched_transactions method
        response = cocoon.cocoon.return_unmatched_transactions(
            self.api_factory, scope, code
        )

        # Assert that there are only two values returned and that the transaction ids and instrument details match
        self.assertEqual(len(response), 2)
        self.assertEqual(
            response[0].get("trd_0003"),
            {
                "Instrument/default/ClientInternal": "THIS_WILL_NOT_RESOLVE_1",
                "Instrument/default/Sedol": "FAKESEDOL1",
                "Instrument/default/Name": "THIS_WILL_NOT_RESOLVE_1",
            },
        )
        self.assertEqual(
            response[1].get("trd_0004"),
            {
                "Instrument/default/ClientInternal": "THIS_WILL_NOT_RESOLVE_2",
                "Instrument/default/Sedol": "FAKESEDOL2",
                "Instrument/default/Name": "THIS_WILL_NOT_RESOLVE_2",
            },
        )

        # Delete the portfolio at the end of the test
        self.api_factory.build(lusid.api.PortfoliosApi).delete_portfolio(
            scope=scope, code=code
        )

    @parameterized.expand(
        [
            [
                "All returned unmatched transactions were in the upload",
                "data/transactions_unmatched_instruments_all_in_upload.csv",
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
            ],
            [
                "No returned unmatched transactions were in the upload",
                "data/transactions_unmatched_instruments_none_in_upload.csv",
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
                transaction_and_instrument_identifiers(trd_0003=False, trd_0004=False),
            ],
            [
                "Some of the returned unmatched transactions were in the upload",
                "data/transactions_unmatched_instruments_some_in_upload.csv",
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=True),
                transaction_and_instrument_identifiers(trd_0003=True, trd_0004=False),
            ],
        ]
    )
    def test_filter_unmatched_transactions_method_only_returns_transaction_ids_originally_present_in_dataframe(
        self,
        _,
        data_frame_path,
        unmatched_transactions,
        expected_filtered_unmatched_transactions,
    ):
        """
        Test that unmatched transactions that were not part of the current load_from_data_frame operation are
        filtered out of the final list.

        :param str _: The name of the test
        :param str data_frame_path: The name of the test data file
        :param list unmatched_transactions: The raw list of unmatched transactions to be filtered
        :param expected_filtered_unmatched_transactions: The expected unmatched transactions appended to the response

        :return: None
        """
        # Load the dataframe
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(data_frame_path))

        # Filter the transactions to remove ones that were not part of the upload
        filtered_unmatched_transactions = cocoon.cocoon.filter_unmatched_transactions(
            data_frame=data_frame,
            mapping_required={"transaction_id": "txn_id"},
            unmatched_transactions=unmatched_transactions,
        )

        self.assertEqual(
            filtered_unmatched_transactions, expected_filtered_unmatched_transactions
        )

    def test_filter_unmatched_transactions_can_paginate_responses_for_2001_transactions_returned(self):
        """
        The GetTransactions API will only return up to 2,000 transactions per request. This test is to verify that
        LPT can handle responses of greater length, expected to be split across multiple pages.

        Returns
        -------
        Nothing.
        """
        scope = "unmatched_transactions_2k_test"
        identifier_mapping = {
            "Isin": "isin",
            "Figi": "figi",
            "ClientInternal": "client_internal",
            "Currency": "currency_transaction",
        }
        mapping_required = {
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
        }
        mapping_optional = {"transaction_currency": "trade_currency"}

        # Load the dataframe
        data_frame = pd.read_csv(Path(__file__).parent.joinpath("data/global-fund-combined-transaction_2001.csv"))

        # Create the portfolio
        portfolio_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "portfolio_code",
                "display_name": "portfolio_code",
                "base_currency": "$GBP",
            },
            file_type="portfolio",
            mapping_optional={"created": "created_date"},
        )

        # Assert that the portfolio was created without any issues
        self.assertEqual(len(portfolio_response.get("portfolios").get("errors")), 0)

        # Load in the transactions
        transactions_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="transactions",
            identifier_mapping=identifier_mapping,
            batch_size=None,
            return_unmatched_identifiers=True,
        )

        self.assertGreater(len(transactions_response["transactions"]["success"]), 0)
        self.assertEqual(len(transactions_response["transactions"]["errors"]), 0)

        # Assert that the unmatched_identifiers returned are as expected
        self.assertEqual(
            len(transactions_response["transactions"].get("unmatched_identifiers", [])),
            2001
        )

        # Delete the portfolio at the end of the test
        for portfolio in portfolio_response.get("portfolios").get("success"):
            self.api_factory.build(lusid.api.PortfoliosApi).delete_portfolio(
                scope=scope, code=portfolio.id.code
            )



