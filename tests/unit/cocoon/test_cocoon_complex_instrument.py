import unittest
from pathlib import Path
import pandas as pd

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
import lusid.models as models
from lusid.utilities import ApiClientFactory
from lusidtools import logger
from datetime import datetime
import pytz


class ComplexInstrumentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")
        cls.instruments_api = cls.api_factory.build(lusid.api.InstrumentsApi)

        # Set test parameters as class methods
        cls.scope = "TestScope"
        cls.mapping_required = {
            "name": "Name",
            "definition.start_date": "Issue Date",
            "definition.maturity_date": "Maturity Date",
            "definition.dom_ccy": "Currency",
            "definition.instrument_type": "Instrument Type",
            "definition.principal": "Principal",
            "definition.coupon_rate": "Coupon",
            "definition.identifiers.ClientInternal": "Client Internal",
            "definition.flow_conventions.currency": "Currency",
            "definition.flow_conventions.payment_frequency": "Payment Frequency",
            "definition.flow_conventions.day_count_convention": "Day Count Convention",
            "definition.flow_conventions.roll_convention": "Roll Convention",
            "definition.flow_conventions.payment_calendars": "Payment Calendars",
            "definition.flow_conventions.reset_calendars": "Reset Calendars",
            "definition.flow_conventions.settle_days": "Settlement Days",
            "definition.flow_conventions.reset_days": "Reset Days",
        }
        cls.mapping_optional = {}
        cls.identifier_mapping = {"Isin": "ISIN", "ClientInternal": "Client Internal"}
        cls.file_name = "data/global-fund-fixed-income-master.csv"

    # Set the expected result object using LUSID sdk
    def expected_response(self):
        instrument_definition = models.Bond(
            start_date=datetime(2002, 2, 28, tzinfo=pytz.utc),
            maturity_date=datetime(2032, 3, 1, tzinfo=pytz.utc),
            dom_ccy="USD",
            coupon_rate=0.07,
            principal=500000000,
            flow_conventions=models.FlowConventions(
                # coupon payment currency
                currency="USD",
                # semi-annual coupon payments
                payment_frequency="6M",
                # using an Actual/365 day count convention (other options : Act360, ActAct, ...
                day_count_convention="Thirty360",
                # modified following rolling convention (other options : ModifiedPrevious, NoAdjustment, EndOfMonth,...)
                roll_convention="MF",
                # no holiday calendar supplied
                payment_calendars=["US"],
                reset_calendars=["US"],
                settle_days=2,
                reset_days=2,
            ),
            identifiers={"clientInternal": "imd_34534538"},
            instrument_type="Bond",
        )

        instrument_request = {
            "ClientInternal: imd_34534538": models.InstrumentDefinition(
                # instrument display name
                name="DISNEY_7_2032",
                # unique instrument identifier
                identifiers={
                    "ClientInternal": models.InstrumentIdValue("imd_34534538"),
                    "Isin": models.InstrumentIdValue("US25468PBW59"),
                },
                # instrument definition
                definition=instrument_definition,
            )
        }

        return self.instruments_api.upsert_instruments(instrument_request)

    def test_load_from_dataframe_bond(self) -> None:
        """
            Test that instruments can be loaded successfully using load_from_data_frame()

            :param str scope: The scope of the portfolios to load the transactions into
            :param str file_name: The name of the test data file
            :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
            :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
            :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
            :param list[str] property_columns: The columns to create properties for
            :param str properties_scope: The scope to add the properties to

            :return: None
            """
        # Load parameters
        scope = self.scope
        mapping_required = self.mapping_required
        mapping_optional = self.mapping_optional
        identifier_mapping = self.identifier_mapping
        file_name = self.file_name
        # Load expected outcome for an instrument
        expected_outcome = self.expected_response()

        # Load data frame with instrument data
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="instruments",
            identifier_mapping=identifier_mapping,
            instrument_name_enrichment=True,
        )

        self.assertEqual(
            first=sum(
                [
                    len(response.values)
                    for response in responses["instruments"]["success"]
                ]
            ),
            second=len(data_frame["Client Internal"].unique()),
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

        # Clean and aggregate successful responses into a dictionary
        combined_successes = {
            correlation_id: instrument
            for response in responses["instruments"]["success"]
            for correlation_id, instrument in response.values.items()
        }

        # Check instrument definition in response matches expected outcome
        for key in expected_outcome.values.keys():
            response_obj = combined_successes[key]
            self.assertEqual(
                response_obj.instrument_definition,
                expected_outcome.values[key].instrument_definition,
            )

    def test_load_from_dataframe_with_unsupported_inst_type(self) -> None:
        """
            Tests that an invalid instrument_type raises a ValueError in load_from_data_frame()

            :param str scope: The scope of the portfolios to load the transactions into
            :param str file_name: The name of the test data file
            :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
            :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
            :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
            :param list[str] property_columns: The columns to create properties for
            :param str properties_scope: The scope to add the properties to

            :return: None
            """

        # Load parameters
        scope = self.scope
        mapping_required = self.mapping_required
        mapping_optional = self.mapping_optional
        identifier_mapping = self.identifier_mapping
        file_name = self.file_name

        # Load data frame with instrument data
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))
        # Overwrite "Instrument Type" to an unsupported value
        data_frame["Instrument Type"].replace("Bond", "Foo", inplace=True)

        # Check that ValueError is raised
        with self.assertRaises(ValueError):
            cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope=scope,
                data_frame=data_frame,
                mapping_required=mapping_required,
                mapping_optional=mapping_optional,
                file_type="instruments",
                identifier_mapping=identifier_mapping,
                instrument_name_enrichment=True,
            )


if __name__ == "__main__":
    unittest.main()
