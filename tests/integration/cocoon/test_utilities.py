import unittest

from lusidfeature import lusid_feature

import lusidtools.cocoon as cocoon
from parameterized import parameterized
from lusidtools import logger


class CocoonUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature("T14-1", "T14-2")
    @parameterized.expand(
        [
            ["Standard Base URL", "https://fbn-prd.lusid.com/api"],
            ["Base URL with forward slash suffix", "https://fbn-prd.lusid.com/api/"],
        ]
    )
    def test_get_swagger_dict_success(self, _, api_url):

        swagger_dict = cocoon.utilities.get_swagger_dict(api_url=api_url)

        self.assertTrue(expr=isinstance(swagger_dict, dict))

    @lusid_feature("T14-3")
    def test_get_swagger_dict_fail(self):

        with self.assertRaises(ValueError):
            cocoon.utilities.get_swagger_dict(api_url="https://fbn-prd.lusid.com")
