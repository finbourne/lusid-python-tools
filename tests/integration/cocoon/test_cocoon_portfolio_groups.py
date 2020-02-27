import unittest
from pathlib import Path
import pandas as pd
import json
import lusid
import lusid.models as models
from lusidtools import cocoon as cocoon
from lusidtools.cocoon.utilities import create_scope_id
from lusidtools import logger


class CocoonTestPortfolioGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        cls.scope = create_scope_id()
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

    def test_multiple_port_groups_multiple_portfolios(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio groups with multiple portfolios.
        """

        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath("data/port_group_tests/portfolio_group.csv")
        )

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=data_frame,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
            file_type="portfolio_group",
            property_columns=[],
            properties_scope=None,
        )

        if responses.get("portfolio_groups").get("errors") != []:
            for error in responses["portfolio_groups"]["errors"]:
                print(json.loads(error.body)["title"])

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

    def test_one_port_groups_no_portfolios(self) -> None:

        """
        Description:
        ------------
        Here we test adding one new portfolio group with no portfolios.
        """

        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/portfolio_group_no_portfolio.csv"
            )
        )

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=data_frame,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional={},
            file_type="portfolio_group",
            property_columns=[],
            properties_scope=None,
        )

        if responses.get("portfolio_groups").get("errors") != []:
            for error in responses["portfolio_groups"]["errors"]:
                print(json.loads(error.body)["title"])

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0]._id,
            second=lusid.models.ResourceId(scope=self.scope, code="PG_TEST3A"),
        )

    def test_multiple_port_groups_no_portfolios(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio group with no portfolios.
        """

        data_frame = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/portfolio_groups_no_portfolio.csv"
            )
        )

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=data_frame,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional={},
            file_type="portfolio_group",
            property_columns=[],
            properties_scope=None,
        )

        if responses.get("portfolio_groups").get("errors") != []:
            for error in responses["portfolio_groups"]["errors"]:
                print(json.loads(error.body)["title"])

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][1]._id,
            second=lusid.models.ResourceId(scope=self.scope, code="PG_TEST5B"),
        )

    def test_portfolio_not_exist(self):

        """
        Description:
        ------------
        Here we test attempting to add a portfolio which does not exist to a portfolio group.
        """

        test_df = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/portfolio_group_portfolio_not_exist.csv"
            )
        )

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=test_df,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
            file_type="portfolio_group",
            property_columns=[],
            properties_scope=None,
        )

        if responses.get("portfolio_groups").get("errors") != []:
            for error in responses["portfolio_groups"]["errors"]:
                print(json.loads(error.body)["title"])

        self.assertEqual(
            json.loads(responses["portfolio_groups"]["errors"][0].body)["name"],
            "PortfolioNotFound",
        )

    def test_duplicate_portfolio(self):

        """
        Description:
        ------------
        Here we test attempting to add two of the same portfolios to a portfolio group.
        """

        duplicate_df = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/portfolio_group_duplicate_portfolio.csv"
            )
        )

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=duplicate_df,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
            file_type="portfolio_group",
            property_columns=[],
            properties_scope=None,
        )

        if responses.get("portfolio_groups").get("errors") != []:
            for error in responses["portfolio_groups"]["errors"]:
                print(json.loads(error.body)["title"])

        duplicate_df["unique_combinations_for_test"] = (
            duplicate_df["PortGroupCode"]
            + "-"
            + duplicate_df["FundCode"]
            + "-"
            + duplicate_df["Scope"]
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
            len(duplicate_df["unique_combinations_for_test"].unique()),
        )

    def test_port_group_update(self):

        """
        Description:
        ------------
        Here we test updating a portfolio group which already exists.
        """

        update_df = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/portfolio_group_update.csv"
            )
        )

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=self.scope,
            data_frame=update_df,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
            file_type="portfolio_group",
            property_columns=[],
            properties_scope=None,
        )

        if responses.get("portfolio_groups").get("errors") != []:
            for error in responses["portfolio_groups"]["errors"]:
                print(json.loads(error.body)["title"])

        first_group_series = update_df.iloc[0]
        first_group_resoruce_id = lusid.models.ResourceId(
            scope=first_group_series["Scope"], code=first_group_series["FundCode"]
        )

        get_first_group_response_portfolios = (
            self.api_factory.build(lusid.api.PortfolioGroupsApi)
            .get_portfolio_group(
                scope=self.scope, code=first_group_series["PortGroupCode"]
            )
            .portfolios
        )

        self.assertIn(first_group_resoruce_id, get_first_group_response_portfolios)
        self.assertNotIn(
            lusid.models.ResourceId(scope="test_scope_20200225", code="TEST_COM5"),
            get_first_group_response_portfolios,
        )
