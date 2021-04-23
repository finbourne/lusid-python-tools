import unittest
from pathlib import Path
import pandas as pd
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
from datetime import datetime
import pytz
from lusidtools.cocoon.utilities import create_scope_id

unique_properties_scope = create_scope_id()


def expected_response(property_scope="TestPropertiesScope1"):
    return {
        "instruments": lusid.models.UpsertInstrumentsResponse(
            values={
                "ClientInternal: imd_34534539": lusid.models.Instrument(
                    lusid_instrument_id="LUIDUnknown",
                    version=lusid.models.Version(
                        as_at_date=datetime.now(pytz.UTC),
                        effective_from=datetime.now(pytz.UTC),
                    ),
                    name="USTreasury_6.875_2025",
                    identifiers={
                        "ClientInternal": "imd_34534539",
                        "Figi": "BBG000DQQNJ8",
                        "Isin": "US912810EV62",
                    },
                    state="Active",
                    properties=[
                        lusid.models.ModelProperty(
                            effective_from=datetime.min.replace(tzinfo=pytz.UTC),
                            effective_until=datetime.max.replace(tzinfo=pytz.UTC),
                            key=f"Instrument/{property_scope}/currency",
                            value=lusid.models.PropertyValue(label_value="USD"),
                        ),
                        lusid.models.ModelProperty(
                            effective_from=datetime.min.replace(tzinfo=pytz.UTC),
                            effective_until=datetime.max.replace(tzinfo=pytz.UTC),
                            key=f"Instrument/{property_scope}/moodys_rating",
                            value=lusid.models.PropertyValue(label_value="Aa2"),
                        ),
                    ],
                )
            },
            failed={},
        )
    }


class CocoonTestsInstruments(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature(
        "T4-1", "T4-2", "T4-3", "T4-4", "T4-5", "T4-6", "T4-7", "T4-8", "T4-9"
    )
    @parameterized.expand(
        [
            [
                "A standard successful load of instruments",
                "TestScope1",
                "data/global-fund-combined-instrument-master.csv",
                {"name": "instrument_name"},
                {},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "A standard successful load of instruments with string index",
                "TestScope1",
                "data/global-fund-combined-instrument-master-string-index.csv",
                {"name": "instrument_name"},
                {},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "A standard successful load of instruments with duplicates in index",
                "TestScope1",
                "data/global-fund-combined-instrument-master-duplicate-index.csv",
                {"name": "instrument_name"},
                {},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "A standard successful load of instruments with unique properties scope",
                "TestScope1",
                "data/global-fund-combined-instrument-master.csv",
                {"name": "instrument_name"},
                {},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                f"TestPropertiesScope1_{unique_properties_scope}",
                expected_response(f"TestPropertiesScope1_{unique_properties_scope}"),
            ],
            [
                "Additional attributes in the both required and optional mapping that don't exist in LUSID but are in the dataframe",
                "TestScope1",
                "data/global-fund-combined-instrument-master.csv",
                {"name": "instrument_name", "position": "instrument_name"},
                {"transaction.cost": "currency"},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "A different way of specifying the identifiers",
                "TestScope1",
                "data/global-fund-combined-instrument-master.csv",
                {"name": "instrument_name"},
                {},
                {
                    "Instrument/default/Figi": "figi",
                    "Isin": "isin",
                    "Instrument/default/ClientInternal": "client_internal",
                },
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "Providing a default for a required field which has some missing values",
                "TestScope1",
                "data/global-fund-combined-instrument-master-missing-names.csv",
                {
                    "name": {
                        "column": "instrument_name",
                        "default": "<NameCurrentlyUnknown>",
                    }
                },
                {},
                {
                    "Instrument/default/Figi": "figi",
                    "Isin": "isin",
                    "Instrument/default/ClientInternal": "client_internal",
                },
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "Providing a default for a required field which has some missing values",
                "TestScope1",
                "data/global-fund-combined-instrument-master-missing-names.csv",
                {
                    "name": {
                        "column": "instrument_name",
                        "default": "<NameCurrentlyUnknown>",
                    }
                },
                {},
                {
                    "Instrument/default/Figi": "figi",
                    "Isin": "isin",
                    "Instrument/default/ClientInternal": "client_internal",
                },
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
            [
                "Loading instruments with duplicate rows for the same instrument",
                "TestScope1",
                "data/global-fund-combined-instrument-master-duplicates.csv",
                {"name": "instrument_name"},
                {},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ],
        ]
    )
    def test_load_from_data_frame_instruments_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        expected_outcome,
    ) -> None:
        """
        Test that instruments can be loaded successfully

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
            file_type="instruments",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

        self.assertEqual(
            first=sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["success"]
                ]
            ),
            second=len(data_frame["client_internal"].unique()),
        )

        self.assertEqual(
            first=sum(
                [
                    len(response.failed)
                    for response in responses["instruments"]["success"]
                ]
            ),
            second=0,
        )

        # Assert that by no unmatched_identifiers are returned in the response for instruments
        self.assertFalse(responses["instruments"].get("unmatched_identifiers", False))

        combined_successes = {
            correlation_id: instrument
            for response in responses["instruments"]["success"]
            for correlation_id, instrument in response.values.items()
        }

        self.assertTrue(
            expr=all(
                sorted(combined_successes[key].properties, key=lambda p: p.key)
                == sorted(value.properties, key=lambda p: p.key)
                for key, value in expected_outcome["instruments"].values.items()
            )
        )

    @lusid_feature("T4-10")
    @parameterized.expand(
        [
            [
                "Using instrument name enrichment to populate missing names",
                "TestScope1",
                "data/global-fund-combined-instrument-master-missing-names.csv",
                {
                    "name": {
                        "column": "instrument_name",
                        "default": "<NameCurrentlyUnknown>",
                    }
                },
                {},
                {
                    "Instrument/default/Figi": "figi",
                    "Isin": "isin",
                    "Instrument/default/ClientInternal": "client_internal",
                },
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ]
        ]
    )
    def test_load_from_data_frame_instruments_enrichment_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        expected_outcome,
    ) -> None:
        """
        Test that instruments can be loaded successfully

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
            file_type="instruments",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            instrument_name_enrichment=True,
        )

        self.assertEqual(
            first=sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=responses["instruments"]["success"][0]
            .values["ClientInternal: imd_43535553"]
            .name,
            second="BP PLC",
        )

        self.assertEqual(
            first=sum(
                [
                    len(response.failed)
                    for response in responses["instruments"]["success"]
                ]
            ),
            second=0,
        )

        combined_successes = {
            correlation_id: instrument
            for response in responses["instruments"]["success"]
            for correlation_id, instrument in response.values.items()
        }

        self.assertTrue(
            expr=all(
                sorted(combined_successes[key].properties, key=lambda p: p.key)
                == sorted(value.properties, key=lambda p: p.key)
                for key, value in expected_outcome["instruments"].values.items()
            )
        )

    @lusid_feature("T4-11")
    @parameterized.expand(
        [
            [
                "A standard successful load of instruments with whitespace",
                "TestScope2",
                "data/global-fund-combined-instrument-master-with-whitespace.csv",
                "data/global-fund-combined-instrument-master.csv",
                {"name": "instrument_name"},
                {},
                {"Figi": "figi", "Isin": "isin", "ClientInternal": "client_internal"},
                ["s&p rating", "moodys_rating", "currency"],
                "TestPropertiesScope1",
                expected_response(),
            ]
        ]
    )
    def test_load_from_data_frame_instruments_with_strip(
        self,
        _,
        scope,
        file_name_with_whitespace,
        file_name_clean,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        expected_outcome,
    ) -> None:
        """
        Test that instruments can be loaded successfully with whitespace, and validates that the

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name_with_whitespace: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list[str] property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param any expected_outcome: The expected outcome

        :return: None
        """

        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath(file_name_with_whitespace)
        )
        data_frame_true = pd.read_csv(Path(__file__).parent.joinpath(file_name_clean))
        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="instruments",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            remove_white_space=True,
        )

        self.assertEqual(
            len(data_frame_true["client_internal"].unique()),
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

        combined_successes = {
            correlation_id: instrument
            for response in responses["instruments"]["success"]
            for correlation_id, instrument in response.values.items()
        }

        self.assertTrue(
            expr=all(
                sorted(combined_successes[key].properties, key=lambda p: p.key)
                == sorted(value.properties, key=lambda p: p.key)
                for key, value in expected_outcome["instruments"].values.items()
            )
        )

    @lusid_feature("T4-12")
    def test_load_instrument_properties(self,):
        data_frame = pd.DataFrame(
            {
                "instrument_name": [
                    "BP_LondonStockEx_BP",
                    "Glencore_LondonStockEx_GLEN",
                    "TESCO_LondonStockEx_TSCO",
                ],
                "figi": ["BBG000C05BD1", "BBG001MM1KV4", "BBG000BF46Y8"],
                "isin": ["BBG000C05BD1", "JE00B4T3BW64", "GB0008847096"],
                "client_internal": ["imd_43535553", "imd_34534555", "imd_34634673"],
            }
        )

        scope = "TestScope1"
        mapping_required = {"name": "instrument_name"}
        identifier_mapping = {
            "Figi": "figi",
            "Isin": "isin",
            "ClientInternal": "client_internal",
        }

        # set up initial instruments
        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional={},
            file_type="instruments",
            identifier_mapping=identifier_mapping,
        )

        self.assertGreater(len(responses["instruments"]["success"]), 0)

        properties_df = pd.DataFrame(
            {
                "isin": ["BBG000C05BD1", "GG00B4L84979", "GB0031509804"],
                "category": ["Oil & Gas", "Mining", "Retail"],
            }
        )

        properties_required_mapping = {"identifier": "isin", "identifier_type": "$Isin"}

        result = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=properties_df,
            mapping_required=properties_required_mapping,
            mapping_optional={},
            file_type="instrument_property",
            properties_scope=scope,
            property_columns=["category"],
            batch_size=1,
        )

        errors = [
            error
            for batch in result["instrument_propertys"]["errors"]
            for error in batch
        ]
        successes = [
            success
            for batch in result["instrument_propertys"]["success"]
            for success in batch
        ]

        self.assertEqual(len(errors), 0)
        self.assertGreater(len(successes), 0)

        # Assert that by no unmatched_identifiers are returned in the response for instrument_propertys
        self.assertFalse(result["instrument_propertys"].get("unmatched_identifiers", False))

    @lusid_feature("T4-13")
    def test_load_instrument_properties_with_missing_instruments(self):
        properties_df = pd.DataFrame({"isin": ["blah"], "category": ["Oil & Gas"]})

        properties_required_mapping = {"identifier": "isin", "identifier_type": "$Isin"}

        scope = "TestScope1"

        result = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=properties_df,
            mapping_required=properties_required_mapping,
            mapping_optional={},
            file_type="instrument_property",
            properties_scope=scope,
            property_columns=["category"],
        )

        errors = [
            error
            for batch in result["instrument_propertys"]["errors"]
            for error in batch
        ]
        successes = [
            success
            for batch in result["instrument_propertys"]["success"]
            for success in batch
        ]

        self.assertEqual(len(errors), 0)
        self.assertEqual(len(successes), 0)

    @lusid_feature("T4-15", "T4-16", "T4-17", "T4-18", "T4-19")
    @parameterized.expand(
        [
            [
                "load only lookthrough instruments",
                Path(__file__).parent.joinpath(
                    "data/lookthrough_instr_tests/load_lookthrough_instrument.csv"
                ),
                {
                    "identifier_mapping": {"ClientInternal": "client_internal",},
                    "required": {"name": "instrument_name"},
                    "optional": {
                        "look_through_portfolio_id.scope": "lookthrough_scope",
                        "look_through_portfolio_id.code": "lookthrough_code",
                    },
                },
            ],
            [
                "load mixed lookthrough instruments",
                Path(__file__).parent.joinpath(
                    "data/lookthrough_instr_tests/mixed_lookthrough_instruments.csv"
                ),
                {
                    "identifier_mapping": {"ClientInternal": "client_internal",},
                    "required": {"name": "instrument_name"},
                    "optional": {
                        "look_through_portfolio_id.scope": "lookthrough_scope",
                        "look_through_portfolio_id.code": "lookthrough_code",
                    },
                },
            ],
            [
                "load mixed lookthrough instruments with default scope",
                Path(__file__).parent.joinpath(
                    "data/lookthrough_instr_tests/mixed_instruments_default_scope.csv"
                ),
                {
                    "identifier_mapping": {"ClientInternal": "client_internal",},
                    "required": {"name": "instrument_name"},
                    "optional": {
                        "look_through_portfolio_id.scope": "$test-lookthrough-loading-lusidtools",
                        "look_through_portfolio_id.code": "lookthrough_code",
                    },
                },
            ],
            [
                "load mixed lookthrough instruments with default scope with multiple portfolios",
                Path(__file__).parent.joinpath(
                    "data/lookthrough_instr_tests/mixed_lookthrough_instruments_multiple portfolios.csv"
                ),
                {
                    "identifier_mapping": {"ClientInternal": "client_internal",},
                    "required": {"name": "instrument_name"},
                    "optional": {
                        "look_through_portfolio_id.scope": "$test-lookthrough-loading-lusidtools",
                        "look_through_portfolio_id.code": "lookthrough_code",
                    },
                },
            ],
            [
                "multiple_instruments_with_same_portfolio",
                Path(__file__).parent.joinpath(
                    "data/lookthrough_instr_tests/multiple_instruments_with_same_portfolio.csv"
                ),
                {
                    "identifier_mapping": {"ClientInternal": "client_internal",},
                    "required": {"name": "instrument_name"},
                    "optional": {
                        "look_through_portfolio_id.scope": "$test-lookthrough-loading-lusidtools",
                        "look_through_portfolio_id.code": "lookthrough_code",
                    },
                },
            ],
        ]
    )
    def test_load_instrument_lookthrough(self, _, df, mapping):

        if "default scope" in _:
            self.skipTest("Default parameter using '$' is not supported")

        scope = "test-lookthrough-loading-lusidtools"
        df = pd.read_csv(df)

        # replace lookthrough scope
        df = df.replace({"replace_scope": scope})

        # populate portfolio ids with random codes
        codes = {
            row["client_internal"]: create_scope_id(use_uuid=True)
            if "Portfolio" in row["instrument_name"]
            else row["client_internal"]
            for index, row in df.iterrows()
        }
        df = df.replace(codes)

        # create portfolios
        [
            self.api_factory.build(lusid.api.TransactionPortfoliosApi).create_portfolio(
                scope=scope,
                create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                    display_name=row["client_internal"],
                    description=row["client_internal"],
                    code=row["client_internal"],
                    base_currency="USD",
                ),
            )
            if "Portfolio" in row["instrument_name"]
            else None
            for index, row in df.drop_duplicates("client_internal").iterrows()
        ]

        # Upsert lookthrough instrument of portfolio
        instr_response = cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=df,
            mapping_required=mapping["required"],
            mapping_optional=mapping["optional"],
            file_type="instruments",
            identifier_mapping=mapping["identifier_mapping"],
            property_columns=[],
        )

        # check successes, errors and instrument lookthrough codes
        self.assertEqual(
            len(instr_response["instruments"]["success"][0].values.values()), len(df)
        )
        self.assertEqual(len(instr_response["instruments"]["errors"]), 0)

        # check lookthrough code on response
        [
            self.assertEqual(
                instr_response["instruments"]["success"][0]
                .values[f"ClientInternal: {row['client_internal']}"]
                .lookthrough_portfolio.code,
                row["lookthrough_code"],
            )
            if "id" not in row["client_internal"]
            else None
            for index, row in df.iterrows()
        ]

        # tear down this test
        [
            self.api_factory.build(lusid.api.PortfoliosApi).delete_portfolio(
                scope=scope, code=code
            )
            if "id" not in code
            else None
            for code in list(codes.values())
        ]
        [
            self.api_factory.build(lusid.api.InstrumentsApi).delete_instrument(
                "ClientInternal", CI
            )
            for CI in list(df["client_internal"])
        ]
