import unittest
from unittest import  mock
import os
from lusidtools.lpt import connect_lusid


class ConnectLusidTests(unittest.TestCase):

    @mock.patch.dict(os.environ, {"FBN_LUSID_API_URL": ""})
    def test_missing_one_config(self):
        config = {}
        config["api"] = {}
        config["api"]["tokenUrl"] = "abc"
        config["api"]["username"] = "abc"
        config["api"]["password"] = os.environ["FBN_PASSWORD"]
        config["api"]["clientId"] = "abc"
        config["api"]["clientSecret"] = "abc"
        config["api"]["apiUrl"] = None

        missing_config = connect_lusid.check_for_missing_config(config)

        self.assertEqual(1, len(missing_config))
        self.assertEqual([{'Env variable': 'FBN_LUSID_API_URL', 'Secrets file key': 'apiUrl'}], missing_config)