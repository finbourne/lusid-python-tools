import unittest
from pathlib import Path
import pandas as pd
import json
import lusid
import lusid.models as models
from lusidtools import cocoon as cocoon
from lusidtools.cocoon.utilities import create_scope_id
import datetime
from dateutil.tz import tzutc
import logging

logger = logging.getLogger()


class CocoonTestPortfolioGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        cls.portfolio_scope = create_scope_id()
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.unique_portfolios = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/test_1_pg_create_with_portfolios.csv"
            )
        )["FundCode"].tolist()

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

            for code in cls.unique_portfolios:
                cls.api_factory.build(
                    lusid.api.TransactionPortfoliosApi
                ).create_portfolio(
                    scope=cls.portfolio_scope,
                    create_transaction_portfolio_request=create_portfolio_model(code),
                )
        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                logger.error(f"The portfolio {code} already exists")

    def log_error_requests_title(cls, domain, responses):
        if len(responses.get(domain, {}).get("errors", [])) > 0:
            for error in responses[domain]["errors"]:
                return logger.error(json.loads(error.body)["title"])

    def csv_to_data_frame_with_scope(cls, csv, scope):
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(csv))
        data_frame["Scope"] = scope
        return data_frame

    def cocoon_load_from_dataframe(
        cls,
        scope,
        data_frame,
        mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
        property_columns=[],
        properties_scope=None,
    ):

        return cocoon.cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional=mapping_optional,
            file_type="portfolio_group",
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

    def test_01_pg_create_with_portfolios(self) -> None:

        """
        Test description:
        ------------------
        Here we test adding multiple new portfolio groups with multiple portfolios.

        Expected outcome:
        -----------------
        We expect one successful request/response per portfolio group with multiple portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_1_pg_create_with_portfolios.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Test that there is a successful request per line in the dataframe
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

        # Test that all the portfolios in the dataframe are in the request
        self.assertEqual(
            first=sorted(
                [
                    code.to_dict()
                    for code in responses["portfolio_groups"]["success"][0].portfolios
                ],
                key=lambda item: item.get("code"),
            ),
            second=sorted(
                [
                    lusid.models.ResourceId(
                        code=data_frame["FundCode"][1], scope=data_frame["Scope"][1]
                    ).to_dict(),
                    lusid.models.ResourceId(
                        code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
                    ).to_dict(),
                ],
                key=lambda item: item.get("code"),
            ),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][1].portfolios,
            second=[
                lusid.models.ResourceId(
                    code=data_frame["FundCode"][2], scope=data_frame["Scope"][2]
                )
            ],
        )

    def test_02_pg_create_with_no_portfolio(self) -> None:

        """
        Test description:
        -----------------
        Here we test adding one new portfolio group with no portfolios.

        Expected outcome:
        -----------------
        We expect one successful new portfolio group with no portfolios.

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_2_pg_create_with_no_portfolio.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check that the portfolio group is created

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        # Check that the correct portfolio group code is used
        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
            ),
        )

        # Check that the portfolio group request has now portfolios
        self.assertTrue(
            len(responses["portfolio_groups"]["success"][0].portfolios) == 0
        )

    def test_03_pg_create_multiple_groups_no_portfolio(self) -> None:

        """
        Test description:
        -----------------
        Here we test adding multiple new portfolio group with no portfolios.

        Expected outcome
        -----------------
        We expect successful requests/responses for multiple new portfolio groups.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_3_pg_create_multiple_groups_no_portfolio.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check that there is a requet per portfolio group

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        # Check that the portfolio group code matches the code in the dataframe

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][1].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[1]
            ),
        )

    def test_04_pg_create_with_portfolio_not_exist(self):

        """
        Test description:
        -----------------
        Here we test attempting to add a portfolio which does not exist to a portfolio group.

        Expected outcome:
        -----------------
        We expect the entire request to fail with a PortfolioNotFound error.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_4_pg_create_with_portfolio_not_exist.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check that LUSID cannot find the portfolio

        self.assertEqual(
            json.loads(responses["portfolio_groups"]["errors"][0].body)["name"],
            "PortfolioNotFound",
        )

        # Check there are no successful requests

        self.assertEqual(len(responses["portfolio_groups"]["success"]), 0)

    def test_05_pg_create_with_duplicate_portfolios(self):

        """
        Test description:
        -----------------
        Here we test attempting to add two of the same portfolios to a portfolio group.

        Expected result:
        ----------------
        We expect that each unique portfolio gets added and duplicates should be ignored.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_5_pg_create_with_duplicate_portfolios.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        data_frame.drop_duplicates(inplace=True)

        # Check that there is a request for each unique portfolio

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

        # Check that a request is generated with unqiue portfolio only

        self.assertEqual(
            first=sorted(
                [
                    code.to_dict()
                    for code in responses["portfolio_groups"]["success"][0].portfolios
                ],
                key=lambda item: item.get("code"),
            ),
            second=sorted(
                [
                    lusid.models.ResourceId(
                        code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
                    ).to_dict(),
                    lusid.models.ResourceId(
                        code=data_frame["FundCode"][1], scope=data_frame["Scope"][1]
                    ).to_dict(),
                ],
                key=lambda item: item.get("code"),
            ),
        )

    def test_06_pg_create_duplicate_port_group(self):

        """
        Test description:
        -----------------
        Here we test create two of the same portfolio groups

        Expected results
        -----------------
        We expect one successful requesy for the portfolio group

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_6_pg_create_duplicate_port_group.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        # Check for one successful request

        self.assertEqual(len(responses["portfolio_groups"]["success"]), 1)

        # Check the successful request has same code as dataframe portfolio group

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
            ),
        )

    def test_08_pg_add_bad_portfolio(self):

        """
        Description:
        ------------
        Here we test add a portfolio which does not exist to a current portfolio group.

        Expected results:
        -----------------
        The portfolio group is returned without the portfolios added.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_8_pg_add_bad_portfolio.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertTrue(len(responses["portfolio_groups"]["errors"]) == 0)

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
            ),
        )

    def test_07_pg_create_with_properties(self) -> None:

        """
        Test description:
        -----------------
        Here we test creating a portfolio group with properties.

        Expected output:
        ----------------
        The response contains the upserted properties.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_7_pg_create_with_properties.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope,
            data_frame=data_frame,
            property_columns=["location", "MifidFlag"],
            properties_scope=test_case_scope,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        response_with_properties = self.api_factory.build(
            lusid.api.PortfolioGroupsApi
        ).get_group_properties(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0],
        )

        self.assertEqual(
            {
                "PortfolioGroup/"
                + test_case_scope
                + "/location": lusid.models.ModelProperty(
                    key="PortfolioGroup/" + test_case_scope + "/location",
                    value=lusid.models.PropertyValue(label_value="UK"),
                    effective_from=datetime.datetime(1, 1, 1, 0, 0, tzinfo=tzutc()),
                ),
                "PortfolioGroup/"
                + test_case_scope
                + "/MifidFlag": lusid.models.ModelProperty(
                    key="PortfolioGroup/" + test_case_scope + "/MifidFlag",
                    value=lusid.models.PropertyValue(label_value="Y"),
                    effective_from=datetime.datetime(1, 1, 1, 0, 0, tzinfo=tzutc()),
                ),
            },
            response_with_properties.properties,
        )

    def test_09_pg_add_duplicate_portfolio(self) -> None:

        """
        Description:
        ------------
        Here we test adding duplicate portfolios to a portfolio group.

        Expected outcome:
        -----------------
        We expect the one portfolio to be added.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_9_pg_add_duplicate_portfolio.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].portfolios[0],
            second=lusid.models.ResourceId(
                code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
            ),
        )

    def test_10_pg_add_no_new_portfolio(self) -> None:

        """
        Test description:
        ------------
        Here we test adding an existing portfolio to portfolio group.

        Expected result:
        ----------------
        The portfolio group response should be returned with one unmodified portfolio.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_10_pg_add_no_new_portfolio.csv",
            self.portfolio_scope,
        )

        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
            values=[
                lusid.models.ResourceId(
                    code=data_frame["FundCode"][0], scope=self.portfolio_scope
                )
            ],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].portfolios[0],
            second=lusid.models.ResourceId(
                code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
            ),
        )

    def test_11_pg_add_bad_and_good_portfolios(self):

        """
        Test description:
        -----------------
        Here we test updating a portfolio group with good and bad portfolios.

        Expected result:
        -----------------
        Good portfolios should be added and bad ones not added.

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_11_pg_add_bad_and_good_portfolios.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        remove_dupe_df = data_frame[~data_frame["FundCode"].str.contains("BAD_PORT")]

        self.assertEqual(
            first=sorted(
                [
                    code.to_dict()
                    for code in responses["portfolio_groups"]["success"][0].portfolios
                ],
                key=lambda item: item.get("code"),
            ),
            second=sorted(
                [
                    lusid.models.ResourceId(
                        code=remove_dupe_df["FundCode"].tolist()[0],
                        scope=self.portfolio_scope,
                    ).to_dict(),
                    lusid.models.ResourceId(
                        code=remove_dupe_df["FundCode"].tolist()[1],
                        scope=self.portfolio_scope,
                    ).to_dict(),
                ],
                key=lambda item: item.get("code"),
            ),
        )

    def test_12_pg_add_portfolios_different_scopes(self) -> None:

        """
        Test description:
        -----------------
        Here we test adding portfolios with multiple scopes.

        Expected outcome:
        -----------------
        Request should be successful - returned with portfolios with multiple scopes.

        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_12_pg_add_portfolios_different_scopes.csv",
            self.portfolio_scope,
        )

        port_scope_for_test = create_scope_id()
        self.api_factory.build(lusid.api.TransactionPortfoliosApi).create_portfolio(
            scope=port_scope_for_test,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name=data_frame["FundCode"][0],
                code=data_frame["FundCode"][0],
                base_currency="GBP",
            ),
        )

        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
            values=[
                lusid.models.ResourceId(
                    code=data_frame["FundCode"][0], scope=port_scope_for_test
                )
            ],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, create_portfolio_group_request=port_group_request,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertTrue(
            expr=all(
                [
                    id in responses["portfolio_groups"]["success"][0].portfolios
                    for id in [
                        lusid.models.ResourceId(
                            code=data_frame["FundCode"][1], scope=data_frame["Scope"][1]
                        ),
                        lusid.models.ResourceId(
                            code=data_frame["FundCode"][0], scope=port_scope_for_test
                        ),
                        lusid.models.ResourceId(
                            code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
                        ),
                        lusid.models.ResourceId(
                            code=data_frame["FundCode"][2], scope=data_frame["Scope"][2]
                        ),
                    ]
                ]
            )
        )
