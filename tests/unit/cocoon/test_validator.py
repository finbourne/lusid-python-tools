import unittest
from lusidtools import logger
from lusidtools.cocoon.validator import Validator
from parameterized import parameterized


class CocoonUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            [
                "Value does exist in allowed values",
                "Portfolio",
                "file_type",
                ["Portfolio", "Transaction"],
            ]
        ]
    )
    def test_check_allowed_value_success(self, _, value, value_name, allowed_values):

        Validator(value, value_name).check_allowed_value(allowed_values)

    @parameterized.expand(
        [
            [
                "Value does not exist in allowed values",
                "Reference",
                "file_type",
                ["Portfolio", "Transaction"],
                ValueError,
            ],
            [
                "Value does not exist in allowed values with empty list",
                "Reference",
                "file_type",
                [],
                ValueError,
            ],
            [
                "Value is not a string",
                1,
                "file_type",
                ["Portfolio", "Transaction"],
                ValueError,
            ],
            [
                "Value is a list",
                ["Portfolio", "Transaction"],
                "file_type",
                ["Portfolio", "Transaction"],
                ValueError,
            ],
            [
                "Value is a dictionary",
                {"type": "portfolio"},
                "file_type",
                ["Portfolio", "Transaction"],
                ValueError,
            ],
        ]
    )
    def test_check_allowed_value_exception(
        self, _, value, value_name, allowed_values, expected_exception
    ):

        with self.assertRaises(expected_exception):
            Validator(value, value_name).check_allowed_value(allowed_values)

    @parameterized.expand(
        [
            [
                "A plural with a single 's' on the end",
                "Transactions",
                "file_type",
                "Transaction",
            ],
            [
                "A plural with a two 's' on the end",
                "Transactionss",
                "file_type",
                "Transaction",
            ],
            ["Already singular", "Transaction", "file_type", "Transaction"],
            ["Not a string", 1, "file_type", 1],
        ]
    )
    def test_make_singular(self, _, value, value_name, expected_outcome):

        singular = Validator(value, value_name).make_singular()

        self.assertIsInstance(singular, Validator)

        self.assertEqual(first=singular.value, second=expected_outcome)

    @parameterized.expand(
        [
            ["All upper case", "TRANSACTIONS", "file_type", "transactions"],
            ["Mixed case", "TrAnSaCTIons", "file_type", "transactions"],
            ["Already lower case", "transactions", "file_type", "transactions"],
            ["Not a string", 1, "file_type", 1],
        ]
    )
    def test_make_lower(self, _, value, value_name, expected_outcome):

        lower_case = Validator(value, value_name).make_lower()

        self.assertIsInstance(lower_case, Validator)

        self.assertEqual(first=lower_case.value, second=expected_outcome)

    @parameterized.expand(
        [
            ["None value provided with default", None, "batch_size", 10, 10],
            [
                "None value provided with None as default",
                None,
                "batch_size",
                None,
                None,
            ],
            ["Not None provided with default", 3, "batch_size", 10, 3],
        ]
    )
    def test_set_default_value_if_none(
        self, _, value, value_name, default, expected_outcome
    ):

        updated_value = Validator(value, value_name).set_default_value_if_none(default)

        self.assertIsInstance(updated_value, Validator)

        self.assertEqual(first=updated_value.value, second=expected_outcome)

    @parameterized.expand(
        [
            ["Override Flag Set to True", 10, "batch_size", True, 20, 20],
            ["Override Flag Set to False", 10, "batch_size", False, 20, 10],
            [
                "Override Flag Set to FalseTrue via expression",
                10,
                "batch_size",
                "Portfolio" == "Transaction",
                20,
                10,
            ],
        ]
    )
    def test_override_value(
        self, _, value, value_name, override_flag, override_value, expected_outcome
    ):

        updated_value = Validator(value, value_name).override_value(
            override_flag, override_value
        )

        self.assertIsInstance(updated_value, Validator)

        self.assertEqual(first=updated_value.value, second=expected_outcome)

    @parameterized.expand(
        [
            [
                "Single dictionary with None values",
                {"type": None, "code": "portfolio_code"},
                "optional_mapping",
                {"code": "portfolio_code"},
            ],
            [
                "Nested dictionary with None values at the bottom",
                {
                    "type": {"column": None, "default": "Reference"},
                    "code": "portfolio_code",
                },
                "required_mapping",
                {
                    "type": {"column": None, "default": "Reference"},
                    "code": "portfolio_code",
                },
            ],
            [
                "Dictionary with no None values",
                {"type": "Buy", "code": "portfolio_code"},
                "optional_mapping",
                {"type": "Buy", "code": "portfolio_code"},
            ],
        ]
    )
    def test_discard_dict_keys_none_value(self, _, value, value_name, expected_outcome):

        update_dict = Validator(value, value_name).discard_dict_keys_none_value()

        self.assertIsInstance(update_dict, Validator)

        self.assertEqual(first=update_dict.value, second=expected_outcome)

    @parameterized.expand(
        [
            [
                "Single dictionary with None values",
                {"type": None, "code": "portfolio_code"},
                "optional_mapping",
                [None, "portfolio_code"],
            ],
            [
                "Nested dictionary with None values at the bottom",
                {
                    "type": {"column": None, "default": "Reference"},
                    "code": "portfolio_code",
                },
                "required_mapping",
                [{"column": None, "default": "Reference"}, "portfolio_code"],
            ],
            [
                "Dictionary with no None values",
                {"type": "Buy", "code": "portfolio_code"},
                "optional_mapping",
                ["Buy", "portfolio_code"],
            ],
        ]
    )
    def test_get_dict_values(self, _, value, value_name, expected_outcome):

        dict_values = Validator(value, value_name).get_dict_values()

        self.assertIsInstance(dict_values, Validator)

        self.assertEqual(first=dict_values.value, second=expected_outcome)

    @parameterized.expand(
        [
            [
                "List with some members containing first character",
                ["$Code", "$Buy", "Sell"],
                "code_list",
                "$",
                ["Sell"],
            ],
            [
                "List with non members containing first character",
                ["Code", "Buy", "Sell"],
                "code_list",
                "$",
                ["Code", "Buy", "Sell"],
            ],
            [
                "First character has more than one character",
                ["$Code", "$Buy", "Sell"],
                "code_list",
                "$$",
                ["$Code", "$Buy", "Sell"],
            ],
        ]
    )
    def test_filter_list_using_first_character(
        self, _, value, value_name, first_character, expected_outcome
    ):

        updated_list = Validator(value, value_name).filter_list_using_first_character(
            first_character
        )

        self.assertIsInstance(updated_list, Validator)

        self.assertEqual(first=updated_list.value, second=expected_outcome)

    @parameterized.expand(
        [
            [
                "List is a subset",
                ["Portfolio", "Transaction"],
                "file_types",
                ["Portfolio", "Transaction", "Holding"],
                "all_file_types",
            ]
        ]
    )
    def test_check_subset_of_list_success(
        self, _, value, value_name, superset, superset_name
    ):

        Validator(value, value_name).check_subset_of_list(superset, superset_name)

    @parameterized.expand(
        [
            [
                "List is not a subset",
                ["Portfolio", "Transaction", "Quote"],
                "file_types",
                ["Portfolio", "Transaction", "Holding"],
                "all_file_types",
                ValueError,
            ]
        ]
    )
    def test_check_subset_of_list_exception(
        self, _, value, value_name, superset, superset_name, expected_exception
    ):

        with self.assertRaises(expected_exception):
            Validator(value, value_name).check_subset_of_list(superset, superset_name)

    @parameterized.expand(
        [
            [
                "Lists have no intersection",
                ["Portfolio", "Transaction", "Quote"],
                "file_types",
                ["Instrument", "Holding"],
                "other_file_types",
            ]
        ]
    )
    def test_check_no_intersection_with_list_success(
        self, _, value, value_name, other_list, list_name
    ):

        Validator(value, value_name).check_no_intersection_with_list(
            other_list, list_name
        )

    @parameterized.expand(
        [
            [
                "Lists have at one intersection",
                ["Portfolio", "Transaction", "Quote"],
                "file_types",
                ["Instrument", "Holding", "Quote"],
                "other_file_types",
                ValueError,
            ]
        ]
    )
    def test_check_no_intersection_with_list_exception(
        self, _, value, value_name, other_list, list_name, expected_exception
    ):

        with self.assertRaises(expected_exception):
            Validator(value, value_name).check_no_intersection_with_list(
                other_list, list_name
            )

    @parameterized.expand(
        [
            [
                "Dictionary missing 'source' key",
                [{"foo": "bar"}],
                "The value [{'foo': 'bar'}] provided in property_columns is invalid.",
                "{'foo': 'bar'} does not contain the mandatory 'source' key."
            ],
            [
                "Dictionary with 'source' that's not a string",
                [{"source": 2}],
                "The value [{'source': 2}] provided in property_columns is invalid.",
                "2 in {'source': 2} is not a string."
            ],
            [
                "Non string",
                [1],
                "The value [1] provided in property_columns is invalid",
                "1 is not a string or dictionary."
            ],
            [
                "Multiple errors",
                [1, {"foo": "bar"}, {"source": [5]}],
                "The value [1, {'foo': 'bar'}, {'source': [5]}] provided in property_columns is invalid",
                "1 is not a string or dictionary, " +
                "{'foo': 'bar'} does not contain the mandatory 'source' key, " +
                "[5] in {'source': [5]} is not a string."
            ]
        ]
    )
    def test_check_entries_are_strings_or_dict_containing_key_invalid_values(self, _, value,
                                                                             expected_message_part1,
                                                                             expected_message_part2):
        with self.assertRaises(ValueError) as context:
            Validator(value, "property_columns").check_entries_are_strings_or_dict_containing_key("source")

        self.assertTrue(expected_message_part1 in str(context.exception), str(context.exception))
        self.assertTrue(expected_message_part2 in str(context.exception), str(context.exception))

    @parameterized.expand(
        [
            ["Dictionary with 'source'", [{"source": "foo"}]],
            ["String", ["foo"]],
            ["Multiple values", ["foo", {"source": "bar"}]]
        ]
    )
    def test_check_entries_are_strings_or_dict_containing_key_valid_values(self, _, value):
        Validator(value, "property_columns").check_entries_are_strings_or_dict_containing_key("source")