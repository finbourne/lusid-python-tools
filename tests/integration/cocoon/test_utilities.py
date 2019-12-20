import unittest
from parameterized import parameterized
import lusidtools.cocoon as cocoon
from lusidtools import logger


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
