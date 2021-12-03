import unittest
from parameterized import parameterized
import lusid
import pathlib
import tests.unit.apps.test_data.test_transactions as txn_test_data
import lusidtools.apps.flush_transactions as flush
import datetime
from dateutil.tz import tzutc
import logging


class FlushAndFillIntegrationTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(level=logging.INFO)
        cls.scope = "rjh-test-scope-01"
        cls.code = "rjhTestPortfolio01"
        secrets_path = pathlib.Path().resolve().parent.parent / "secrets.json"
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_path
        )

        cls.transaction_portfolios_api = cls.api_factory.build(lusid.api.TransactionPortfoliosApi)
        response = cls.transaction_portfolios_api.create_portfolio(cls.scope, {"displayName": "TestPortfolio",
                                                                               "description": "Portfolio for flush tests",
                                                                               "code": cls.code,
                                                                               "created": "2018-03-05T12:00:00.0000000+00:00",
                                                                               "baseCurrency": "USD",
                                                                               })

    @parameterized.expand([
        [
            "outside_the_test_data_time",
            datetime.datetime(2020, 2, 20, 0, 0, tzinfo=tzutc()),
            datetime.datetime(2020, 2, 25, 0, 0, tzinfo=tzutc()),
            txn_test_data.txn_count
        ],
        [
            "inside_the_test_data_time",
            datetime.datetime(2020, 2, 10, 0, 0, tzinfo=tzutc()),
            datetime.datetime(2020, 2, 25, 0, 0, tzinfo=tzutc()),
            0
        ],
    ])
    def test_flush_between_dates(self, _, fromDate, toDate, expected_txn_count):
        # Upsert Test Transaction Data
        self.transaction_portfolios_api.upsert_transactions(self.scope, self.code, txn_test_data.test_transactions_lst)

        flush.flush(self.scope, self.code, fromDate, toDate, self.api_factory)

        new_transactions = self.transaction_portfolios_api.get_transactions(self.scope,
                                                                            self.code,
                                                                            limit=5000)
        observed_count = len(new_transactions.values)
        next_transaction_page = new_transactions.next_page
        while next_transaction_page is not None:
            page_of_transactions = self.transaction_portfolios_api.get_transactions(self.scope,
                                                                                    self.code,
                                                                                    limit=5000,
                                                                                    page=next_transaction_page)
            observed_count = observed_count + len(page_of_transactions.values)
            next_transaction_page = page_of_transactions.next_page

        self.assertEqual(observed_count, expected_txn_count)

    @classmethod
    def tearDownClass(cls) -> None:
        portfolios_api = cls.api_factory.build(lusid.api.PortfoliosApi)
        response = portfolios_api.delete_portfolio(cls.scope, cls.code)
