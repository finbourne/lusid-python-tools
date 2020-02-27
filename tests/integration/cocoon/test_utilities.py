import unittest
import lusidtools.cocoon as cocoon
import lusid.models as models

from parameterized import parameterized
from lusidtools import logger
from lusidtools.cocoon.utilities import group_request_into_one


class CocoonUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            ["Standard Base URL", "https://fbn-prd.lusid.com/api"],
            ["Base URL with forward slash suffix", "https://fbn-prd.lusid.com/api/"],
        ]
    )
    def test_get_swagger_dict_success(self, _, api_url):

        swagger_dict = cocoon.utilities.get_swagger_dict(api_url=api_url)

        self.assertTrue(expr=isinstance(swagger_dict, dict))

    @parameterized.expand(
        [["Base URL with missing /api path", "https://fbn-prd.lusid.com", ValueError]]
    )
    def test_get_swagger_dict_fail(self, _, api_url, expected_exception):

        with self.assertRaises(expected_exception):
            cocoon.utilities.get_swagger_dict(api_url=api_url)


    def test_group_request_into_one_list(self):

        requests = [
            models.CreatePortfolioGroupRequest(
                code="PORT_GROUP1",
                display_name="Portfolio Group 1",
                values=[models.ResourceId(scope="TEST1", code="PORT1")],
                properties={"test": models.ModelProperty(key="test", value="prop1"),
                            "test2": models.ModelProperty(key="test", value="prop2")},
                sub_groups=None,
                description=None,
                created="2019-01-01"),
            models.CreatePortfolioGroupRequest(
                code="PORT_GROUP1",
                display_name="Portfolio Group 1",
                values=[models.ResourceId(scope="TEST1", code="PORT2")],
                sub_groups=None,
                properties={"test3": models.ModelProperty(key="test", value="prop3"),
                            "test2": models.ModelProperty(key="test", value="prop4")},
                description=None,
                created="2019-01-01"),
            models.CreatePortfolioGroupRequest(
                code="PORT_GROUP1",
                display_name="Portfolio Group 1",
                values=[models.ResourceId(scope="TEST1", code="PORT3")],
                sub_groups=None,
                description=None,
                created="2019-01-01"),
            models.CreatePortfolioGroupRequest(
                code="PORT_GROUP1",
                display_name="Portfolio Group 1",
                values=[models.ResourceId(scope="TEST1", code="PORT4")],
                sub_groups=None,
                description=None,
                created="2019-01-01")]

        # Run list tests

        list_grouped_request = group_request_into_one("CreatePortfolioGroupRequest", requests, ["values"])
        self.assertEqual(len(list_grouped_request.values), 4)
        self.assertEqual(list_grouped_request.values[3].code, "PORT4")

        #  Run dict tests

        dict_grouped_request = group_request_into_one("CreatePortfolioGroupRequest", requests, ["properties"])
        self.assertEqual(len(dict_grouped_request.properties), 3)
        self.assertEqual(list(dict_grouped_request.properties.values())[0].value, "prop1")
