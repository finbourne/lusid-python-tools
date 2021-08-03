import unittest
import uuid
from pathlib import Path

import lusid
import pandas as pd

from lusidtools import cocoon as cocoon
from lusidtools import logger


class CocoonTestsFormatInstruments(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

        cls.mapping_required = {"name": "instrument_name"}
        cls.mapping_optional = {
            "look_through_portfolio_id.scope": "lookthrough_scope",
            "look_through_portfolio_id.code": "lookthrough_code",
        }
        cls.identifier_mapping = {
            "Instrument/default/Figi": "figi",
            "Instrument/default/ClientInternal": "client_internal",
        }

    def test_all_fields(self):
        scope = f"TestScope-{uuid.uuid4()}"

        portfolios = pd.DataFrame(
            {"code": "Child", "name": "Child Portfolio", "currency": "GBP", "date": "01/01/2020"},
            {"code": "Parent", "name": "Parent Portfolio", "currency": "GBP", "date": "01/01/2020"}
        )

        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=portfolios,
            mapping_required={
                "code": "code",
                "display_name": "name",
                "base_currency": "currency",
            },
            file_type="portfolio",
            mapping_optional={"created": "date"},
        )

        instruments = pd.DataFrame([{
            "instrument_name": "BP_LondonStockEx_BP",
            "client_internal": "imd_43535553",
            "currency": "GBP",
            "figi": "BBG000C05BD1",
            "lookthrough_scope": scope,
            "lookthrough_code": "Child"
        }])

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=instruments,
            mapping_required=self.mapping_required,
            mapping_optional=self.mapping_optional,
            file_type="instruments",
            identifier_mapping=self.identifier_mapping,
            instrument_name_enrichment=True,
        )

        response = cocoon.format_instruments_response(responses, data_entity_details=True)
        success_df = response[0]

        self.assertEqual(success_df.shape, (1, 14))
        self.assertEqual(scope, success_df["lookthrough_portfolio.scope"].values[0])
        self.assertEqual("Child", success_df["lookthrough_portfolio.code"].values[0])

    def test_identifier(self):
        scope = f"TestScope-{uuid.uuid4()}"

        instruments = pd.DataFrame([{
            "instrument_name": "BP_LondonStockEx_BP",
            "client_internal": "imd_43535553",
            "currency": "GBP",
            "figi": "BBG000C05BD1"
        }])

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=instruments,
            mapping_required=self.mapping_required,
            mapping_optional={},
            file_type="instruments",
            identifier_mapping=self.identifier_mapping,
            instrument_name_enrichment=True,
        )

        response = cocoon.format_instruments_response(responses)
        success_df = response[0]

        self.assertEqual(success_df.shape, (1, 1))
        self.assertEqual("ClientInternal: imd_43535553", success_df[0].values[0])
