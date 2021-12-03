import unittest
from parameterized import parameterized
import lusid
import pathlib
import tests.unit.apps.test_data.test_transactions as txn_test_data
import lusidtools.apps.flush_transactions as flush
import logging


class FlushAndFillUnitTests(unittest.TestCase):

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

    @parameterized.expand([
        [
            "full-files-batched-by-character",
            4000,
            txn_test_data.test_txnids_data,
            txn_test_data.test_batched_data
        ],
    ])
    def test_transaction_batcher_by_character_count(self, _, maxCharacterCount, whole_txn_set, test_batched_list):
        batched_data = flush.transaction_batcher_by_character_count(self.scope,
                                                                    self.code,
                                                                    self.api_factory.api_client.configuration.host,
                                                                    whole_txn_set,
                                                                    maxCharacterCount)

        self.assertEqual(batched_data, test_batched_list)

    @parameterized.expand([
        [
            "id-test-0",
            txn_test_data.test_txns_for_id,
            txn_test_data.test_txnids_data
        ],
    ])
    def test_get_ids_from_txns(self, _, transactions, test_id_lst):
        self.assertEqual(flush.get_ids_from_txns(transactions), test_id_lst)
