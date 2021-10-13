import os
import unittest
from pathlib import Path
import pandas as pd
import lusid
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id
from parameterized import parameterized


class CocoonTestsReferencePortfolios(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.portfolios_api = cls.api_factory.build(lusid.api.PortfoliosApi)
        cls.unique_id = create_scope_id()
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))
        cls.scope = "ModelFundTest"
        cls.file_name = "data/reference-portfolio/reference-test.csv"

    @lusid_feature("T11-1", "T11-2", "T11-3")
    @parameterized.expand(
        [
            [
                "Load a reference portfolio with required attributes only",
                "data/reference-portfolio/reference-test.csv",
                {"code": "FundCode", "display_name": "display_name"},
                {
                    "description": "description",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                [],
            ],
            [
                "Load a reference portfolio with required attributes AND properties",
                "data/reference-portfolio/reference-test.csv",
                {"code": "FundCode", "display_name": "display_name"},
                {
                    "description": "description",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                ["strategy", "custodian"],
            ],
            [
                "Load duplicate reference portfolios",
                "data/reference-portfolio/reference-test-duplicates.csv",
                {"code": "FundCode", "display_name": "display_name"},
                {
                    "description": "description",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                [],
            ],
        ]
    )
    def test_load_from_data_frame_attributes_and_properties_success(
        self, _, file_name, mapping_required, mapping_optional, property_columns,
    ) -> None:
        """
        Test that a reference portfolio can be loaded successfully

        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param list[str] property_columns: The columns to create properties for
        :param any expected_outcome: The expected outcome

        :return: None
        """

        unique_id = create_scope_id()
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))
        data_frame.drop_duplicates(inplace=True)
        data_frame["FundCode"] = data_frame["FundCode"] + "-" + unique_id

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="reference_portfolio",
            identifier_mapping={},
            property_columns=property_columns,
            properties_scope=self.scope,
            sub_holding_keys=[],
        )

        # Check that the count of portfolios uploaded equals count of portfolios in DataFrame

        self.assertEqual(
            first=len(responses["reference_portfolios"]["success"]),
            second=len(data_frame),
        )

        # Assert that by no unmatched_identifiers are returned in the response for reference_portfolios
        self.assertFalse(
            responses["reference_portfolios"].get("unmatched_identifiers", False)
        )

        # Check that the portfolio IDs of porfolios uploaded matches the IDs of portfolios in DataFrame

        response_codes = [
            response.id.code
            if isinstance(response, lusid.models.Portfolio)
            else response.origin_portfolio_id.code
            for response in responses["reference_portfolios"]["success"]
        ]

        self.assertEqual(
            first=response_codes,
            second=list(data_frame[mapping_required["code"]].values),
        )

        # Check that properties get added to portfolio

        for portfolio in responses["reference_portfolios"]["success"]:

            get_portfolio = self.portfolios_api.get_portfolio(
                scope=portfolio.id.scope,
                code=portfolio.id.code,
                property_keys=[
                    f"Portfolio/{self.scope}/strategy",
                    f"Portfolio/{self.scope}/custodian",
                ],
            )

            property_keys_from_params = [
                f"Portfolio/{self.scope}/{code}"
                for code in property_columns
                if len(property_columns) > 0
            ]

            self.assertCountEqual(
                [prop for prop in get_portfolio.properties], property_keys_from_params,
            )

    @lusid_feature("T11-4")
    def test_portfolio_missing_attribute(self):

        unique_id = create_scope_id()
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(self.file_name))
        data_frame["FundCode"] = data_frame["FundCode"] + "-" + unique_id

        mapping_required = {"code": "FundCode"}
        mapping_optional = {
            "description": "description",
            "created": "created",
            "base_currency": "base_currency",
        }

        with self.assertRaises(ValueError) as error:

            cocoon.cocoon.load_from_data_frame(
                api_factory=self.api_factory,
                scope=self.scope,
                data_frame=data_frame,
                mapping_required=mapping_required,
                mapping_optional=mapping_optional,
                file_type="reference_portfolio",
                identifier_mapping={},
                property_columns=[],
                properties_scope=self.scope,
                sub_holding_keys=[],
            )

        self.assertEqual(
            error.exception.args[0],
            """The required attributes {'display_name'} are missing from the mapping. Please
                             add them.""",
        )
