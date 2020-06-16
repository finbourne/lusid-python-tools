import unittest
from pathlib import Path
import pandas as pd
from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
from datetime import datetime
import pytz
from lusidtools.cocoon.utilities import create_scope_id

unique_properties_scope = create_scope_id()


class CocoonTestsInstruments(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key=f"Instrument/TestPropertiesScope1_{unique_properties_scope}/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key=f"Instrument/TestPropertiesScope1_{unique_properties_scope}/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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

        combined_successes = {
            correlation_id: instrument
            for response in responses["instruments"]["success"]
            for correlation_id, instrument in response.values.items()
        }

        self.assertTrue(
            expr=all(
                combined_successes[key].properties == value.properties
                for key, value in expected_outcome["instruments"].values.items()
            )
        )

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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                combined_successes[key].properties == value.properties
                for key, value in expected_outcome["instruments"].values.items()
            )
        )

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
                {
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
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/currency",
                                        value=lusid.models.PropertyValue(
                                            label_value="USD"
                                        ),
                                    ),
                                    lusid.models.ModelProperty(
                                        effective_from=datetime(
                                            year=1,
                                            month=1,
                                            day=1,
                                            hour=0,
                                            minute=0,
                                            tzinfo=pytz.UTC,
                                        ),
                                        key="Instrument/TestPropertiesScope1/moodys_rating",
                                        value=lusid.models.PropertyValue(
                                            label_value="Aa2"
                                        ),
                                    ),
                                ],
                            )
                        },
                        failed={},
                    )
                },
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
                combined_successes[key].properties == value.properties
                for key, value in expected_outcome["instruments"].values.items()
            )
        )

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
