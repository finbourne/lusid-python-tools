import unittest
from pathlib import Path
import pandas as pd
import lusid
import lusid.models as models
from lusidtools import cocoon as cocoon
from parameterized import parameterized
from lusidtools import logger


class CocoonTestPortfolioGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

        def create_portfolio_model(code):
            model = models.CreateTransactionPortfolioRequest(
                display_name=code,
                code=code,
                base_currency="GBP",
                description="Paper transaction portfolio",
                created="2020-02-25T00:00:00Z",
            )
            return model

        try:

            for code in ["TEST_COM1", "TEST_COM2", "TEST_COM3", "TEST_COM4"]:
                cls.api_factory.build(
                    lusid.api.TransactionPortfoliosApi
                ).create_portfolio(
                    scope="test_scope_20200225",
                    transaction_portfolio=create_portfolio_model(code),
                )
        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                print(f"The portfolio {code} already exists")

    @parameterized.expand(
        [
            [
                "Portfolio group load",
                "port_group_load_test",
                "data/portfolio_group.csv",
                {"code": "PortGroupCode", "display_name": "PortGroupDisplayName",},
                {"values.scope": "Scope", "values.code": "FundCode",},
                [],
                None,
                None,
            ]
        ]
    )
    def test_multiple_port_groups_multiple_portfolios(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        property_columns,
        properties_scope,
        expected_outcome,
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
            file_type="portfolio_group",
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

        self.assertEqual(
            len(
                [
                    port_group
                    for nested_group in [
                        port_group.portfolios
                        for port_group in responses["portfolio_groups"]["success"]
                    ]
                    for port_group in nested_group
                ]
            ),
            len(data_frame),
        )

    @parameterized.expand(
        [
            [
                "Portfolio group load",
                "port_group_load_test",
                "data/portfolio_group_no_portfolio.csv",
                {"code": "PortGroupCode", "display_name": "PortGroupDisplayName",},
                {},
                [],
                None,
                None,
            ]
        ]
    )
    def test_one_port_groups_no_portfolios(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        property_columns,
        properties_scope,
        expected_outcome,
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
        :param any expected_outcome: The expected outcome
        :return: None
        """
        original_data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=original_data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolio_group",
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(original_data_frame),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0]._id,
            second=lusid.models.ResourceId(
                scope="port_group_load_test", code="PG_TEST_NOPG1"
            ),
        )

    @parameterized.expand(
        [
            [
                "Portfolio group load",
                "port_group_load_test",
                "data/portfolio_groups_no_portfolio.csv",
                {"code": "PortGroupCode", "display_name": "PortGroupDisplayName",},
                {},
                [],
                None,
                None,
            ]
        ]
    )
    def test_multiple_port_groups_no_portfolios(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        property_columns,
        properties_scope,
        expected_outcome,
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
        :param any expected_outcome: The expected outcome
        :return: None
        """
        original_data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=original_data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolio_group",
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(original_data_frame),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][1]._id,
            second=lusid.models.ResourceId(
                scope="port_group_load_test", code="PG_TEST_NOPG2"
            ),
        )
