import unittest
import uuid
from pathlib import Path

import pandas
import pandas as pd
import lusid
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id


class CocoonTestsPortfolios(unittest.TestCase):
    api_factory = None

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature("T6-1", "T6-2", "T6-3", "T6-4", "T6-5", "T6-6", "T6-7", "T6-8")
    @parameterized.expand(
        [
            [
                "Standard load",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
                None,
                None,
            ],
            [
                "Standard load with ~700 portfolios",
                "prime_broker_test",
                "data/metamorph_portfolios-large.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
                None,
                None,
            ],
            [
                "Add in some constants",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": "$AverageCost"},
                {},
                ["base_currency"],
                "operations001",
                None,
                None,
                None,
            ],
            [
                "Standard load with unique properties scope to ensure property creation",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id()}",
                None,
                None,
                None,
            ],
            [
                "Standard load with a single sub-holding-key",
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                ["Strategy"],
                None,
                ["Transaction/operations001/Strategy"],
            ],
            [
                "Standard load with a multiple sub-holding-keys in a unique scope",
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001",
                ["Strategy", "Broker"],
                None,
                [
                    "Transaction/operations001/Strategy",
                    "Transaction/operations001/Broker",
                ],
            ],
            [
                "Standard load with a sub-holding-key with scope specified",
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id(use_uuid=True)}",
                ["Trader/Strategy"],
                None,
                ["Transaction/Trader/Strategy"],
            ],
            [
                "Standard load with a sub-holding-key with domain and scope specified",
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id(use_uuid=True)}",
                ["Transaction/Trader/Strategy"],
                None,
                ["Transaction/Trader/Strategy"],
            ],
            [
                "Standard load with a single sub-holding-key in a different scope",
                f"prime_broker_test_{create_scope_id(use_uuid=True)}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                ["Strategy"],
                f"accountview007",
                ["Transaction/accountview007/Strategy"],
            ],
        ]
    )
    def test_load_from_data_frame_a_portfolios_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
        sub_holdings_key_scope,
        expected_sub_holdings_keys,
    ) -> None:
        """
        Test that portfolios can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict(str, str) mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict(str, str) mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict(str, str) identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list(str) property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param str sub_holdings_key_scope: The scope of the sub holding keys
        :param list(str): Expected sub holding keys

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            sub_holding_keys_scope=sub_holdings_key_scope,
        )

        self.assertEqual(
            first=len(responses["portfolios"]["success"]), second=len(data_frame)
        )

        response_codes = [
            response.id.code
            if isinstance(response, lusid.models.Portfolio)
            else response.origin_portfolio_id.code
            for response in responses["portfolios"]["success"]
        ]

        self.assertEqual(
            first=response_codes,
            second=list(data_frame[mapping_required["code"]].values),
        )

        # Assert that by no unmatched_identifiers are returned in the response for portfolios
        self.assertFalse(responses["portfolios"].get("unmatched_identifiers", False))

        if expected_sub_holdings_keys is None:
            return

        for portfolio_code in response_codes:

            portfolio_details = self.api_factory.build(
                lusid.api.TransactionPortfoliosApi
            ).get_details(scope=scope, code=portfolio_code)

            self.assertSetEqual(
                set(portfolio_details.sub_holding_keys), set(expected_sub_holdings_keys)
            )

    @lusid_feature("T6-9")
    @parameterized.expand(
        [
            [
                "ApiValueError due to invalid scope",
                "p rime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
            ],
        ]
    )
    def test_load_from_data_frame_a_portfolios_failure(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
    ) -> None:
        """
        Test that portfolios can be loaded successfully

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
        from lusid.exceptions import ApiValueError

        with self.assertRaises(ApiValueError):

            responses = (
                cocoon.cocoon.load_from_data_frame(
                    api_factory=self.api_factory,
                    scope=scope,
                    data_frame=data_frame,
                    mapping_required=mapping_required,
                    mapping_optional=mapping_optional,
                    file_type="portfolios",
                    identifier_mapping=identifier_mapping,
                    property_columns=property_columns,
                    properties_scope=properties_scope,
                    sub_holding_keys=sub_holding_keys,
                ),
            )

    @parameterized.expand(
        [
            [
                "Standard load",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {},
                {},
                ["base_currency"],
                "operations0011",
            ]
        ]
    )
    def test_properties_list_strings(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
    ) -> None:
        """
        Test that portfolios can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict(str, str) mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict(str, str) mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict(str, str) identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list(str) property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

        for property_column in property_columns:
            response = self.api_factory.build(
                lusid.api.PropertyDefinitionsApi
            ).get_property_definition(
                domain="Portfolio", scope=properties_scope, code=property_column,
            )
            self.assertEqual(
                f"Portfolio/{properties_scope}/{property_column}", response.key
            )

    @parameterized.expand(
        [
            [
                "Source only",
                f"prime_broker_test__{uuid.uuid4()}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {},
                {},
                {"source": "base_currency"},
                "operations0011",
                "operations0011",
                "base_currency",
            ],
            [
                "Source and target",
                f"prime_broker_test_{uuid.uuid4()}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {},
                {},
                {"source": "base_currency", "target": "base_currency2"},
                "operations0011",
                "operations0011",
                "base_currency2",
            ],
            [
                "Scope",
                f"prime_broker_test_{uuid.uuid4()}",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {},
                {},
                {"source": "base_currency", "target": "base_currency2", "scope": "foo"},
                "operations0011",
                "foo",
                "base_currency2",
            ],
        ]
    )
    def test_properties_dicts(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_column,
        properties_scope,
        expected_property_scope,
        expected_property_code,
    ) -> None:
        """
        Test that portfolios can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict(str, str) mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict(str, str) mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict(str, str) identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list(str) property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=[property_column],
            properties_scope=properties_scope,
        )

        response = self.api_factory.build(
            lusid.api.PropertyDefinitionsApi
        ).get_property_definition(
            domain="Portfolio",
            scope=expected_property_scope,
            code=expected_property_code,
        )
        self.assertEqual(
            f"Portfolio/{expected_property_scope}/{expected_property_code}",
            response.key,
        )

        for code in data_frame["FundCode"].values:
            response = self.api_factory.build(
                lusid.api.PortfoliosApi
            ).get_portfolio_properties(scope=scope, code=code)
            self.assertIsNotNone(
                response.properties.get(
                    f"Portfolio/{expected_property_scope}/{expected_property_code}"
                )
            )

    @parameterized.expand(
        [
            [
                "Invalid dictionary",
                [{"foo": "bar"}],
                "The value [{'foo': 'bar'}] provided in property_columns is invalid. "
                "{'foo': 'bar'} does not contain the mandatory 'source' key.",
            ],
            [
                "Non string or dictionary",
                [1],
                "The value [1] provided in property_columns is invalid. "
                "1 is not a string or dictionary.",
            ],
        ]
    )
    def test_invalid_properties(
        self, _, property_columns, expected_error_message
    ) -> None:
        with self.assertRaises(ValueError) as context:
            cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope="foo",
                data_frame=pandas.DataFrame(),
                mapping_required={},
                mapping_optional={},
                file_type="portfolios",
                identifier_mapping={},
                property_columns=property_columns,
                properties_scope="bar",
            )

        self.assertEqual(expected_error_message, str(context.exception))
