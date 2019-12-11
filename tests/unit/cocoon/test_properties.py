import unittest
from parameterized import parameterized
import lusid
from pathlib import Path
import lusidtools.cocoon as cocoon
import pandas as pd
import numpy as np
from lusidtools import logger


class CocoonPropertiesTests(unittest.TestCase):
    class MockApiFactory(lusid.utilities.ApiClientFactory):
        """
        This is a mock of the lusid.utilities.ApiClientFactory class
        """

        def build(self, api):
            """

            :param lusid.api api: The api to mock

            :return: mock(lusid.api): The mocked api
            """

            if api == lusid.PropertyDefinitionsApi:
                return self.MockPropertyDefinitionsApi()

        class MockPropertyDefinitionsApi:
            """
            A mock of the lusid.PropertyDefinitionsApi
            """

            def create_property_definition(
                self, definition, **kwargs
            ) -> lusid.models.PropertyDefinition:
                """
                This mocks the creation of a portfolio definition

                :param lusid.models.CreatePropertyDefinitionRequest definition: The create property defintion request
                :param kwargs:
                :return: lusid.models.PropertyDefinition: The property defintion of the created property
                """
                return lusid.models.PropertyDefinition(
                    key="{}/{}/{}".format(
                        definition.domain, definition.scope, definition.code
                    ),
                    data_type_id=lusid.models.ResourceId(
                        scope=definition.data_type_id.scope,
                        code=definition.data_type_id.code,
                    ),
                )

            def get_property_definition(
                self, domain, scope, code, **kwargs
            ) -> lusid.models.PropertyDefinition:
                """
                This mocks the call to get a property definition

                :param domain: The domain of the property
                :param scope: The scope of the property
                :param code: The code of the property
                :param kwargs:

                :return: lusid.models.PropertyDefinition any: The property definition of the property if it exists
                """
                # Construct the property key
                property_key = "{}/{}/{}".format(domain, scope, code)

                # A static representation of the property definitions that exist
                property_keys_in_existance = {
                    "Instrument/default/Figi": lusid.models.ResourceId(
                        scope="system", code="string"
                    ),
                    "Transaction/default/TradeToPortfolioRate": lusid.models.ResourceId(
                        scope="system", code="number"
                    ),
                    "Transaction/Operations/Strategy": lusid.models.ResourceId(
                        scope="system", code="string"
                    ),
                    "Holding/Operations/Currency": lusid.models.ResourceId(
                        scope="system", code="currency"
                    ),
                }

                # If the property exists return the defintion, else raise an exception
                if property_key in list(property_keys_in_existance.keys()):
                    return lusid.models.PropertyDefinition(
                        key=property_key,
                        data_type_id=property_keys_in_existance[property_key],
                    )
                else:
                    raise lusid.exceptions.ApiException

    @classmethod
    def setUpClass(cls) -> None:
        # Use a mock of the lusid.ApiClientFactory
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = cls.MockApiFactory(api_secrets_filename=secrets_file)
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            ["Instrument/default/Figi", [True, "string"]],
            ["Instrument/default/PropertyThatDoesNotExist", [False, None]],
            ["Transaction/default/TradeToPortfolioRate", [True, "number"]],
            ["Transaction/Operations/Strategy", [True, "string"]],
            ["Holding/Operations/Currency", [True, "currency"]],
        ]
    )
    def test_check_property_definitions_exist_in_scope_single(
        self, property_key, expected_outcomes
    ) -> None:
        """
        Tests that checking for a property definition in a single scope works as expected. The call to LUSID
        is mocked here

        :param str property_key: The property key to check for
        :param list[bool, str] expected_outcomes: Whether or not the property is expected to exist and if it does its type

        :return: None
        """

        (
            property_existence,
            property_type,
        ) = cocoon.properties.check_property_definitions_exist_in_scope_single(
            api_factory=self.api_factory, property_key=property_key
        )

        self.assertEqual(property_existence, expected_outcomes[0])
        self.assertEqual(property_type, expected_outcomes[1])

    @parameterized.expand(
        [
            # Check for two properties which don't exist and one that does with a type of string
            [
                "default",
                "Instrument",
                pd.DataFrame(
                    data={
                        "Figi": ["BBG009QYHGN8", "BBG00JRKP9P0", "BBG009QYHGN8"],
                        "MoodysCreditRating": ["A2", "A1", "A2"],
                        "Age": [30, 70, 15],
                    },
                ),
                ["Figi", "MoodysCreditRating", "Age"],
                [
                    ["MoodysCreditRating", "Age"],
                    pd.DataFrame(
                        data={
                            "Figi": ["BBG009QYHGN8", "BBG00JRKP9P0", "BBG009QYHGN8"],
                            "MoodysCreditRating": ["A2", "A1", "A2"],
                            "Age": [30, 70, 15],
                        },
                    ),
                ],
            ],
            # Check for one property that exists with a currency type and one that does not exist
            [
                "Operations",
                "Holding",
                pd.DataFrame(
                    data={
                        "Currency": ["GBP", "USD", "AUD"],
                        "Notional": [True, False, True],
                    }
                ),
                ["Currency", "Notional"],
                [
                    ["Notional"],
                    pd.DataFrame(
                        data={
                            "Currency": ["GBP", "USD", "AUD"],
                            "Notional": [True, False, True],
                        }
                    ),
                ],
            ],
        ]
    )
    def test_check_property_definitions_exist_in_scope(
        self, scope, domain, data_frame, property_columns, expected_outcome
    ) -> None:
        """
        Tests that checking of the existence of a set of property definitions in a scope works as expected

        :param str scope: The scope of the properties to check for
        :param str domain: The domain of the properties to check for
        :param pd.DataFrame data_frame: The dataframe containing the column headers and data to use to determine data types
        :param list[str]: The list of property columns to check for
        :param list[list[str], pd.DataFrame] expected_outcome: The expected result
        :return: None
        """

        (
            missing_columns,
            updated_data_frame,
        ) = cocoon.properties.check_property_definitions_exist_in_scope(
            api_factory=self.api_factory,
            scope=scope,
            domain=domain,
            data_frame=data_frame,
            property_columns=property_columns,
        )

        self.assertEqual(first=len(missing_columns), second=len(set(missing_columns)))

        self.assertEqual(first=set(missing_columns), second=set(expected_outcome[0]))

        self.assertTrue(expr=updated_data_frame.equals(expected_outcome[1]))

    @parameterized.expand(
        [
            # Test that a missing property is created as expected
            [
                "CreditRating",
                "Instrument",
                pd.DataFrame(
                    data=[
                        {
                            "instrument_name": "GlobalCreditFund",
                            "lookthrough_scope": "SingaporeBranch",
                            "lookthrough_code": "PORT_12490FKS9",
                            "format": "json",
                            "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                            "Moodys": "A2",
                        }
                    ]
                ),
                ["Moodys"],
                [
                    {"Moodys": "Instrument/CreditRating/Moodys"},
                    pd.DataFrame(
                        data=[
                            {
                                "instrument_name": "GlobalCreditFund",
                                "lookthrough_scope": "SingaporeBranch",
                                "lookthrough_code": "PORT_12490FKS9",
                                "format": "json",
                                "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                                "Moodys": "A2",
                            }
                        ]
                    ),
                ],
            ],
            # Test that a property with all null values has a definition created as a string not a number
            [
                "CreditRating",
                "Instrument",
                pd.DataFrame(
                    data=[
                        {
                            "instrument_name": "GlobalCreditFund",
                            "lookthrough_scope": "SingaporeBranch",
                            "lookthrough_code": "PORT_12490FKS9",
                            "format": "json",
                            "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                            "S&P": np.nan,
                        }
                    ]
                ),
                ["S&P"],
                [
                    {"S&P": "Instrument/CreditRating/SandP"},
                    pd.DataFrame(
                        data=[
                            {
                                "instrument_name": "GlobalCreditFund",
                                "lookthrough_scope": "SingaporeBranch",
                                "lookthrough_code": "PORT_12490FKS9",
                                "format": "json",
                                "contract": "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                                "S&P": np.nan,
                            }
                        ],
                        dtype=object,
                    ),
                ],
            ],
        ]
    )
    def test_create_property_definitions_from_file(
        self, scope, domain, data_frame, missing_property_columns, expected_outcome
    ) -> None:
        """

        :param str scope: The scope to create the property definitions in
        :param str domain: The domain to create the property definitions in
        :param pd.Series data_frame_dtypes: The dataframe dtypes to add definitions for
        :return: dict property_key_mapping: A mapping of data_frame columns to property keys
        :param [dict, pd.DataFrame] expected_outcome: The expected outcome

        :return: None
        """

        (
            property_key_mapping,
            data_frame_updated,
        ) = cocoon.properties.create_property_definitions_from_file(
            api_factory=self.api_factory,
            scope=scope,
            domain=domain,
            data_frame=data_frame,
            missing_property_columns=missing_property_columns,
        )

        self.assertEqual(first=property_key_mapping, second=expected_outcome[0])

        self.assertTrue(expr=data_frame_updated.equals(expected_outcome[1]))

    @parameterized.expand(
        [
            # Test creating property values in the instrument domain
            [
                pd.Series(data=["A2", "A-"], index=["Moodys", "S&P"]),
                "CreditRating",
                "Instrument",
                pd.Series(data=["object", "object"], index=["Moodys", "S&P"]),
                [
                    lusid.models.PerpetualProperty(
                        key="Instrument/CreditRating/Moodys",
                        value=lusid.models.PropertyValue(label_value="A2"),
                    ),
                    lusid.models.PerpetualProperty(
                        key="Instrument/CreditRating/SandP",
                        value=lusid.models.PropertyValue(label_value="A-"),
                    ),
                ],
            ],
            # Test creating property values in the transaction domain, including a metric value property
            [
                pd.Series(
                    data=["TD_1241247", 30], index=["TraderId", "Rebalancing_Interval"]
                ),
                "Operations",
                "Transaction",
                pd.Series(
                    data=["object", "float64"],
                    index=["TraderId", "Rebalancing_Interval"],
                ),
                {
                    "Transaction/Operations/TraderId": lusid.models.PerpetualProperty(
                        key="Transaction/Operations/TraderId",
                        value=lusid.models.PropertyValue(label_value="TD_1241247"),
                    ),
                    "Transaction/Operations/Rebalancing_Interval": lusid.models.PerpetualProperty(
                        key="Transaction/Operations/Rebalancing_Interval",
                        value=lusid.models.PropertyValue(
                            metric_value=lusid.models.MetricValue(value=30)
                        ),
                    ),
                },
            ],
        ]
    )
    def test_create_property_values(
        self, row, scope, domain, dtypes, expected_outcome
    ) -> None:
        """

        :param pd.Series row:
        :param str scope:
        :param str domain:
        :param pd.Series dtypes:
        :param list[lusid.models.PerpetualProperty] expected_outcome:

        :return: None
        """

        property_values = cocoon.properties.create_property_values(
            row=row, scope=scope, domain=domain, dtypes=dtypes
        )

        self.assertEqual(first=property_values, second=expected_outcome)

    @parameterized.expand(
        [
            [
                "Just a code",
                ["Strategy"],
                "Traders",
                "Transaction",
                ["Transaction/Traders/Strategy"],
            ],
            [
                "A scope and code",
                ["Signal/Strategy"],
                "Traders",
                "Transaction",
                ["Transaction/Signal/Strategy"],
            ],
            [
                "A full key",
                ["Portfolio/Signal/Strategy"],
                "Traders",
                "Transaction",
                ["Portfolio/Signal/Strategy"],
            ],
            [
                "A scope which is the same as the domain",
                ["Transaction/Strategy"],
                "Traders",
                "Transaction",
                ["Transaction/Transaction/Strategy"],
            ],
        ]
    )
    def test_infer_full_property_keys(
        self, _, partial_keys, properties_scope, domain, expected_outcome
    ):

        full_keys = cocoon.properties._infer_full_property_keys(
            partial_keys=partial_keys, properties_scope=properties_scope, domain=domain
        )

        self.assertEqual(
            first=expected_outcome,
            second=full_keys,
            msg="The full keys don't matched the expected outcome",
        )
