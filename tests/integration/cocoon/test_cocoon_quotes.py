import unittest
from pathlib import Path
import numpy as np
import pandas as pd
from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger


class CocoonTestsQuotes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            (
                "Load dates from default",
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"default": "2019-10-28"},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": "$30",
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
            ),
            (
                "Load dates from column",
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"column": "Valuation Date"},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": "$30",
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
            ),
            (
                    "Load dates as pandas datetime nanoseconds",
                    "TestQuotes007",
                    "data/multiplesystems-prices.csv",
                    {
                        "quote_id.effective_at": {
                            "default": pd.Timestamp('2019-10-28')
                        },
                        "quote_id.quote_series_id.provider": {"default": "client"},
                        "quote_id.quote_series_id.instrument_id": "figi",
                        "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                        "quote_id.quote_series_id.quote_type": "$Price",
                        "quote_id.quote_series_id.field": "$Mid",
                    },
                    {
                        "quote_id.quote_series_id.price_source": None,
                        "metric_value.value": "$30",
                        "metric_value.unit": "currency",
                        "lineage": "$CocoonTestInitial",
                    },
                    None,
            ),
        ]
    )
    def test_load_from_data_frame_quotes_success(
        self,
        _,
        scope,
        file_name,
        quotes_mapping_required,
        quotes_mapping_optional,
        expected_outcome,
    ) -> None:
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=quotes_mapping_required,
            mapping_optional=quotes_mapping_optional,
            file_type="quotes",
        )

        self.assertEqual(
            first=sum(
                [len(response.values) for response in responses["quotes"]["success"]]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=sum(
                [len(response.failed) for response in responses["quotes"]["success"]]
            ),
            second=0,
        )

    @parameterized.expand([
        (
                "Load dates with nanoseconds",
                "TestQuotes007",
                "data/multiplesystems-prices.csv",
                {
                    "quote_id.effective_at": {"column": "Valuation Date"},
                    "quote_id.quote_series_id.provider": {"default": "client"},
                    "quote_id.quote_series_id.instrument_id": "figi",
                    "quote_id.quote_series_id.instrument_id_type": {"default": "Figi"},
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.field": "$Mid",
                },
                {
                    "quote_id.quote_series_id.price_source": None,
                    "metric_value.value": "$30",
                    "metric_value.unit": "currency",
                    "lineage": "$CocoonTestInitial",
                },
                None,
        ),
    ])
    def test_load_from_data_frame_quotes_success_from_column_with_numpy_datetime(
        self,
        _,
        scope,
        file_name,
        quotes_mapping_required,
        quotes_mapping_optional,
        expected_outcome,
    ) -> None:

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # replace Valuation date values with numpy DateTime values
        data_frame[quotes_mapping_required["quote_id.effective_at"]["column"]] = \
            [np.array(['2019-09-01T09:31:22.664'], dtype='datetime64[ns]') for _ in range(len(data_frame))]

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=quotes_mapping_required,
            mapping_optional=quotes_mapping_optional,
            file_type="quotes",
        )

        self.assertEqual(
            first=sum(
                [len(response.values) for response in responses["quotes"]["success"]]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=sum(
                [len(response.failed) for response in responses["quotes"]["success"]]
            ),
            second=0,
        )
