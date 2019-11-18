import unittest
from lusidtools import logger
from lusidtools.cocoon.instruments import prepare_key
from parameterized import parameterized


class CocoonUtilitiesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand([
        [
            "Standard full key",
            "Instrument/default/Figi",
            True,
            "Instrument/default/Figi"
        ],
        [
            "Full key specified as short key",
            "Figi",
            True,
            "Instrument/default/Figi"
        ],
        [
            "Full key specified as property",
            "Instrument/PB/Isin",
            True,
            "Instrument/PB/Isin"
        ],
        [
            "Standard short key",
            "Isin",
            False,
            "Isin"
        ],
        [
            "Short key specifed as long key",
            "Instrument/default/Isin",
            False,
            "Isin"
        ]
    ])
    def test_create_identifiers_prepare_key(self, _, identifier_lusid, full_key_format, expected_outcome) -> None:
        """
        Tests that key preparation for identifiers works as expected

        :param _: The name of the test
        :param str identifier_lusid: The LUSID identifier
        :param bool full_key_format: The full key format
        :param str expected_outcome: The expected output key

        :return: None
        """

        output_key = prepare_key(
            identifier_lusid=identifier_lusid,
            full_key_format=full_key_format)


        print (output_key, expected_outcome)

        self.assertEqual(
            output_key,
            expected_outcome
        )