import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from parameterized import parameterized

import lusidtools.cocoon as cocoon
from lusidtools import logger
from tests.unit.cocoon.mock_api_factory import MockApiFactory


class CocoonInvalidPropertiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = MockApiFactory(api_secrets_filename=secrets_file)
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            (
                "Invalid numeric type",
                pd.DataFrame([{"a": 2}]).astype(np.short),
                "The following columns in the data_frame have not been mapped to LUSID data types: {'a': 'int16'}",
            )
        ]
    )
    def test_create_property_definitions_from_file(
        self, _, data_frame, expected_message
    ) -> None:
        with self.assertRaises(TypeError) as context:
            cocoon.properties.create_property_definitions_from_file(
                api_factory=self.api_factory,
                column_to_scope={
                    column: "abc" for column in data_frame.columns.to_list()
                },
                domain="def",
                data_frame=data_frame,
                missing_property_columns=data_frame.columns.to_list(),
            )
        self.assertTrue(
            expected_message in str(context.exception), str(context.exception)
        )

    @parameterized.expand(
        [
            (
                "Invalid numeric type",
                pd.Series(data=[1], index=["a"]),
                pd.Series(data=["int16"], index=["a"]),
                "The following columns in the data_frame have not been mapped to LUSID data types: {'a': 'int16'}",
            )
        ]
    )
    def test_create_property_values(self, _, row, dtypes, expected_message) -> None:
        with self.assertRaises(TypeError) as context:
            cocoon.properties.create_property_values(
                row=row,
                scope="abc",
                domain="def",
                dtypes=dtypes,
                column_to_scope={column: "abc" for column in row},
            )
        self.assertTrue(
            expected_message in str(context.exception), str(context.exception)
        )
