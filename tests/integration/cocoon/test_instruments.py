import unittest
from pathlib import Path
import pandas as pd
import lusid
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
from lusidtools import logger
import asyncio


class CocoonInstrumentsTests(unittest.TestCase):
    api_factory = None

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")
        cls.loop = cocoon.async_tools.start_event_loop_new_thread()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.loop.stop()

    @lusid_feature("T10-1")
    def test_resolve_valid_instruments(self):
        source_ids = ["BBG000C6K6G9", "BBG000C04D57"]
        names = ["Inst1", "Inst2"]

        data = {"ids": source_ids, "is_cash": [None, None]}
        data_expected_value = {
            "inst_name": names,
            "ids": source_ids,
            "is_cash": [None, None],
        }
        required_mapping_expected_value = {"name": "inst_name"}

        identifier_mapping_expected_value = {
            "Figi": "ids",
            "Instrument/default/Currency": "is_cash",
        }

        identifier_mapping = {"Figi": "ids", "Instrument/default/Currency": "is_cash"}

        df = pd.DataFrame(data)
        df_expected_value = pd.DataFrame(data_expected_value)
        upsert_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            mapping_required=required_mapping_expected_value,
            mapping_optional={},
            data_frame=df_expected_value,
            scope="default",
            file_type="instruments",
            identifier_mapping=identifier_mapping_expected_value,
        )

        result = cocoon.instruments.resolve_instruments(
            self.api_factory, data_frame=df, identifier_mapping=identifier_mapping
        )
        result.set_index("ids", inplace=True)

        def validate_luid(luid):
            self.assertTrue(luid.startswith("LUID_"), f"unexpected LUID value: {luid}")

        for i in source_ids:
            validate_luid(result.loc[i]["LusidInstrumentId"])

    @lusid_feature("T10-2")
    def test_resolve_invalid_instrument(self):
        data = {"ids": ["blah"], "is_cash": [False]}

        identifier_mapping = {"Figi": "ids", "Currency": "is_cash"}

        df = pd.DataFrame(data)

        result = cocoon.instruments.resolve_instruments(
            self.api_factory, data_frame=df, identifier_mapping=identifier_mapping
        )

        self.assertFalse(result.iloc[0]["LusidInstrumentId"])

    @lusid_feature("T10-3")
    @parameterized.expand(
        [
            [
                "Standard Out of Box Identifiers",
                [
                    "Figi",
                    "ClientInternal",
                    "QuotePermId",
                    "LusidInstrumentId",
                    "Currency",
                ],
            ]
        ]
    )
    @unittest.skip("Currency holdings return a message instead of LUID-XXXXXX")
    def test_get_unique_identifiers(self, _, expected_outcome) -> None:
        """
        Tests that the unique identifers are correctly retrieved

        :param expected_outcome: The expected outcome

        :return: None
        """
        unique_identifiers = cocoon.instruments.get_unique_identifiers(self.api_factory)
        unique_identifiers.sort()
        expected_outcome.sort()

        self.assertListEqual(unique_identifiers, expected_outcome)

    @lusid_feature("T10-4", "T10-5", "T10-6", "T10-7")
    @parameterized.expand(
        [
            [
                "A standard successful load of instruments",
                "data/global-fund-combined-instrument-master-missing-name.csv",
                {"name": "instrument_name"},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
            ],
            [
                "Constant prefix",
                "data/global-fund-combined-instrument-master-missing-name.csv",
                {"name": "$Unknown"},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
            ],
            [
                "Default specified",
                "data/global-fund-combined-instrument-master-missing-name.csv",
                {"name": {"default": "$Unknown"}},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
            ],
            [
                "Default specified with column name",
                "data/global-fund-combined-instrument-master-missing-name.csv",
                {"name": {"default": "$Unknown", "column": "instrument_name"}},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
            ],
        ]
    )
    @unittest.skip
    def test_enrich_instruments(
        self, _, file_name, mapping_required, instrument_identifier_mapping
    ):

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        data_frame_updated, mapping_required = asyncio.run_coroutine_threadsafe(
            cocoon.instruments.enrich_instruments(
                api_factory=self.api_factory,
                data_frame=data_frame,
                instrument_identifier_mapping=instrument_identifier_mapping,
                mapping_required=mapping_required,
                constant_prefix="$",
            ),
            self.loop,
        ).result()
