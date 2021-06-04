import unittest
from pathlib import Path
import pandas as pd
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
import numpy as np
import concurrent.futures


class CocoonTestsAsyncTools(unittest.TestCase):
    """"
    These tests are to ensure that the Asynchronous Tools work as expected.
    """

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature("T1-1", "T1-2", "T1-3")
    @parameterized.expand(
        [
            [
                "Standard load with ~700 portfolios in 1 batch with max_threads per batch of 5",
                "data/holdings-example-large.csv",
                1,
                5,
            ],
            [
                "Standard load with ~700 portfolios in 5 batches with max_threads per batch of 5",
                "data/holdings-example-large.csv",
                5,
                5,
            ],
            [
                "Standard load with ~700 portfolios in 5 batches with max_threads per batch of 10",
                "data/holdings-example-large.csv",
                5,
                10,
            ],
        ]
    )
    def test_multiple_threads(
        self, _, file_name, number_threads, thread_pool_max_workers
    ):
        """
        This tests different combinations of running load_from_data_frame across multiple threads and configuring
        the max number of workers that each call to load_from_data_frame will use in its thread pool.

        :param str _: The name of the test
        :param str file_name: The name of the test data file working with holdings
        :param int number_threads: The number of threads to split the file load across (the number of times load_from_data_frame is called)
        :param int thread_pool_max_workers: The maximum number of workers per thread pool for each call to load_from_data_frame

        :return: None
        """

        mapping_required = {
            "code": "FundCode",
            "effective_at": "Effective Date",
            "tax_lots.units": "Quantity",
        }

        mapping_optional = {
            "tax_lots.cost.amount": None,
            "tax_lots.cost.currency": "Local Currency Code",
            "tax_lots.portfolio_cost": None,
            "tax_lots.price": None,
            "tax_lots.purchase_date": None,
            "tax_lots.settlement_date": None,
        }

        identifier_mapping = {
            "Isin": "ISIN Security Identifier",
            "Sedol": "SEDOL Security Identifier",
            "Currency": "is_cash_with_currency",
        }

        property_columns = ["Prime Broker"]

        properties_scope = "operations001"

        scope = "prime_broker_test"

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Split the dataframe across the multiple threads
        splitted_arrays = np.array_split(data_frame, number_threads)

        # Create a ThreadPool to run the threads in
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=number_threads)

        # Run each call to load_from_data_frame in a different thread
        futures = [
            executor.submit(
                cocoon.cocoon.load_from_data_frame,
                self.api_factory,
                scope,
                splitted_arrays[i],
                mapping_required,
                mapping_optional,
                "holdings",
                identifier_mapping,
                property_columns,
                properties_scope,
                None,
                True,
                False,
                None,
                False,
                thread_pool_max_workers,
            )
            for i in range(number_threads)
        ]

        # Get the results
        responses = [future.result() for future in futures]

        # Check that the results are as expected
        self.assertGreater(
            sum([len(response["holdings"]["success"]) for response in responses]), 0
        )
        self.assertEqual(
            sum([len(response["holdings"]["success"]) for response in responses]),
            len(data_frame),
        )
        self.assertEqual(
            sum([len(response["holdings"]["errors"]) for response in responses]), 0
        )

        self.assertTrue(
            expr=all(
                isinstance(succcess_response.version, lusid.models.Version)
                for response in responses
                for succcess_response in response["holdings"]["success"]
            )
        )
