import logging
import os
import unittest
from parameterized import parameterized
import datetime
from dateutil.tz import tzutc
from pathlib import Path
from lusid import PortfoliosApi, TransactionPortfoliosApi, PortfolioGroupsApi
import lusid
from lusid.utilities import ApiClientBuilder
import json
from unittest.mock import patch

from tests.unit.apps.test_data import test_transactions as flush_test_data

from lusidtools.apps import (
    load_instruments,
    load_holdings,
    load_transactions,
    load_quotes,
    load_portfolios,
    flush_transactions,
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

        cls.factory = lusid.utilities.ApiClientFactory(api_secrets_filename=cls.secrets)
        cls.transaction_portfolios_api = cls.factory.build(TransactionPortfoliosApi)
        cls.groups_api = cls.factory.build(PortfolioGroupsApi)

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

        cls.portfolios_api = cls.factory.build(PortfoliosApi)
        portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
            scope=cls.testscope
        )

        existing_portfolios = [
            portfolio.id.code for portfolio in portfolios_response.values
        ]
        cls.test_list = [
            "Global-Strategies-SHK",
            "GlobalCreditFund",
            "TestFlushPortfolio",
            "FlushNonExistentPortfolioTest",
            "FlushFailedResponseTestPortfolio",
        ]

        if not all(x in existing_portfolios for x in cls.test_list):
            for portfolio in cls.test_list:
                if portfolio not in existing_portfolios:
                    transaction_portfolio_request1 = lusid.models.CreateTransactionPortfolioRequest(
                        display_name=portfolio,
                        code=portfolio,
                        base_currency="GBP",
                        created="2018-03-05T12:00:00+00:00",
                        sub_holding_keys=[f"Transaction/{cls.testscope}/currency"],
                    )
                    transactions_portfolio_response1 = cls.transaction_portfolios_api.create_portfolio(
                        scope=cls.testscope,
                        create_transaction_portfolio_request=transaction_portfolio_request1,
                    )
                    logging.info(f"created portfolio: {portfolio}")

        # Portfolio Groups Setup
        cls.groups_test_scopes = [
            "test_flush_groups_scope_01",
            "test_flush_groups_scope_02",
            "test_flush_groups_scope_03",
            "test_flush_groups_sub_scope",
        ]
        cls.groups_test_list = [
            "TestFlushPortfolio01",
            "TestFlushPortfolio02",
            "TestFlushPortfolio03",
        ]
        for scope in cls.groups_test_scopes:
            portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
                scope=scope
            )

            existing_portfolios = [
                portfolio.id.code for portfolio in portfolios_response.values
            ]

            if not all(x in existing_portfolios for x in cls.groups_test_list):
                for portfolio in cls.groups_test_list:
                    if portfolio not in existing_portfolios:
                        transaction_portfolio_request1 = lusid.models.CreateTransactionPortfolioRequest(
                            display_name=portfolio,
                            code=portfolio,
                            base_currency="GBP",
                            created="2018-03-05T12:00:00+00:00",
                        )
                        cls.transaction_portfolios_api.create_portfolio(
                            scope=scope,
                            create_transaction_portfolio_request=transaction_portfolio_request1,
                        )
                        logging.info(f"created portfolio: {portfolio}")

            for portfolio in cls.groups_test_list:
                cls.transaction_portfolios_api.upsert_transactions(
                    scope,
                    portfolio,
                    flush_test_data.gen_transaction_data(
                        2, datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc())
                    ),
                )

        group_master_scopes = [
            "test_flush_groups_sub_scope",
            "test_flush_groups_scope_01",
        ]
        for scope in group_master_scopes:
            portfolio_groups_response = cls.groups_api.list_portfolio_groups(
                scope=scope
            )

            existing_groups = [
                portfolio.id.code for portfolio in portfolio_groups_response.values
            ]
            for group in flush_test_data.group_portfolio_requests[scope]:
                code = group["code"]
                if code not in existing_groups:
                    cls.groups_api.create_portfolio_group(
                        scope, create_portfolio_group_request=group
                    )
                    logging.info(f"created group: {code}")

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

    def test_upsert_transactions_with_empty_column_name(self):

        args = self.invalid_args.copy()
        test_data_root = Path(__file__).parent.joinpath("test_data")
        args["file_path"] = test_data_root.joinpath("transactions.csv")
        args["mapping"] = test_data_root.joinpath(
            "mapping_trans_invalid_empty_column.json"
        )

        with self.assertRaises(ValueError) as context:
            load_transactions(args)
        self.assertTrue(
            "The values {''} exist in the identifier_mapping" in str(context.exception)
        )
        self.assertTrue(
            "but do not exist in the DataFrame Columns" in str(context.exception)
        )

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

    @parameterized.expand(
        [
            [
                "outside_the_test_data_time",
                flush_transactions.parse(
                    args=[
                        "testscope0001",
                        "-p",
                        "TestFlushPortfolio",
                        "-s",
                        "2020-02-20T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                    ]
                ),
                6,
            ],
            [
                "partly_inside_the_test_data_time",
                flush_transactions.parse(
                    args=[
                        "testscope0001",
                        "-p",
                        "TestFlushPortfolio",
                        "-s",
                        "2020-02-18T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                    ]
                ),
                4,
            ],
            [
                "inside_the_test_data_time",
                flush_transactions.parse(
                    args=[
                        "testscope0001",
                        "-p",
                        "TestFlushPortfolio",
                        "-s",
                        "2017-02-10T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                    ]
                ),
                0,
            ],
            [
                "outside_the_test_data_time_for_scope",
                flush_transactions.parse(
                    args=[
                        "testscope0001",
                        "-s",
                        "2020-02-20T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                        "--flush_scope"
                    ]
                ),
                6,
            ],
            [
                "partly_inside_the_test_data_time_for_scope",
                flush_transactions.parse(
                    args=[
                        "testscope0001",
                        "-s",
                        "2020-02-18T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                        "--flush_scope"
                    ]
                ),
                4,
            ],
            [
                "inside_the_test_data_time_for_scope",
                flush_transactions.parse(
                    args=[
                        "testscope0001",
                        "-s",
                        "2017-02-10T00:00:00.0000000+00:00",
                        "-e",
                        "2020-02-28T23:59:59.0000000+00:00",
                        "--flush_scope"
                    ]
                ),
                0,
            ],
        ]
    )
    def test_flush_between_dates(self, _, args, expected_txn_count):
        # Upsert Test Transaction Data
        dates = [
            datetime.datetime(2020, 2, 12, 0, 0, tzinfo=tzutc()),
            datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc()),
            datetime.datetime(2020, 2, 19, 15, 0, tzinfo=tzutc()),
        ]

        for date in dates:
            self.transaction_portfolios_api.upsert_transactions(
                self.testscope,
                "TestFlushPortfolio",
                flush_test_data.gen_transaction_data(2, date),
            )
        args.secrets = self.secrets
        flush_transactions.flush(args)
        args.end_date = None
        args.start_date = None
        observed_count = 0
        for txn_list in list(flush_transactions.get_all_txns(args).values()):
            observed_count += len(txn_list)

        self.assertEqual(expected_txn_count, observed_count)

    def test_flush_without_valid_portfolio(self):
        args = flush_transactions.parse(
            args=[
                "testscope0001",
                "-p",
                "support_non_existent_portfolio_tester",
                "-s",
                "2016-03-05T12:00:00+00:00",
                "-e",
                "2017-03-05T12:00:00+00:00",
            ]
        )
        args.secrets = self.secrets

        with self.assertRaises(lusid.exceptions.ApiException) as cm:
            flush_transactions.flush(args)

        exception = cm.exception
        self.assertEqual(json.loads(exception.body)["code"], 109)

    @parameterized.expand([["single-batch-failure", 1], ["3-batch-failure", 3]])
    @patch(
        "lusidtools.apps.flush_transactions.lusid.api.TransactionPortfoliosApi.cancel_transactions"
    )
    def test_flush_with_failed_responses(self, _, test_fail, mock_lusid_cancel_txns):
        args = flush_transactions.parse(
            args=[
                "testscope0001",
                "-p",
                "FlushFailedResponseTestPortfolio",
                "-s",
                "2017-02-10T00:00:00.0000000+00:00",
                "-e",
                "2020-02-28T23:59:59.0000000+00:00",
            ]
        )
        args.secrets = self.secrets
        transactions = flush_test_data.gen_transaction_data(
            150, datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc())
        )

        self.transaction_portfolios_api.upsert_transactions(
            self.testscope, "FlushFailedResponseTestPortfolio", transactions
        )

        batch_count = len(
            flush_transactions.transaction_batcher_by_character_count(
                args.scope,
                args.portfolio,
                self.factory.api_client.configuration.host,
                [txn["transactionId"] for txn in transactions],
            )
        )

        mock_lusid_cancel_txns.side_effect = [
            lusid.exceptions.ApiException for i in range(test_fail)
        ] + [None for _ in range(batch_count - test_fail)]
        success_count, failure_count = flush_transactions.flush(args)
        self.assertEqual(failure_count, test_fail)

    @parameterized.expand(
        [
            [
                "multiple-portfolios-all-filled-same-scope",
                "test_flush_groups_sub_scope",
                "testFlushGroupsClean",
                3,
            ],
            [
                "multiple-portfolios-all-filled-same-scope-no-sub-groups",
                "test_flush_groups_scope_01",
                "testFlushGroupsCleanNoSub",
                3,
            ],
            [
                "multiple-portfolios-with-different-scopes",
                "test_flush_groups_scope_01",
                "testFlushGroupsMixedScopes",
                4,
            ],
            [
                "only-one-portfolio-filled",
                "test_flush_groups_scope_01",
                "testFlushGroupsSingle",
                1,
            ],
            ["empty-group", "test_flush_groups_scope_01", "testFlushGroupsEmpty", 0],
        ]
    )
    def test_flush_with_portfolio_groups(
        self, _, group_scope, group_name, target_success
    ):
        args = flush_transactions.parse(args=[group_scope, "-p", group_name, "--group"])
        args.secrets = self.secrets

        success_count, failure_count = flush_transactions.flush(args)

        self.assertTrue(failure_count == 0)
        self.assertTrue(success_count == target_success)

    @classmethod
    def tearDownClass(cls) -> None:
        for scope in [cls.testscope] + cls.groups_test_scopes:
            portfolios_response = cls.portfolios_api.list_portfolios_for_scope(
                scope=scope
            )

            existing_portfolios = [
                portfolio.id.code for portfolio in portfolios_response.values
            ]

            for portfolio in cls.test_list + cls.groups_test_list:
                if portfolio in existing_portfolios:
                    cls.portfolios_api.delete_portfolio(scope, portfolio)

        # Tear down Groups
        for scope in ["test_flush_groups_scope_01", "test_flush_groups_sub_scope"]:
            portfolio_groups_response = cls.groups_api.list_portfolio_groups(
                scope=scope
            )

            existing_groups = [
                portfolio.id.code for portfolio in portfolio_groups_response.values
            ]

            for group in existing_groups:
                cls.groups_api.delete_portfolio_group(scope=scope, code=group)
