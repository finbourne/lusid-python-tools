import copy
import numpy
import unittest
from datetime import datetime
from pathlib import Path
from typing import Callable
import lusid
import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_series_equal
import pytz
from parameterized import parameterized
from lusidtools import cocoon
from lusidtools.cocoon.utilities import (
    checkargs,
    get_delimiter,
    check_mapping_fields_exist,
    identify_cash_items,
    strip_whitespace,
    create_scope_id,
    default_fx_forward_model,
    update_dict_value,
)
from lusidtools import logger


@checkargs
def checkargs_list(a_list: list):
    return isinstance(a_list, list)


@checkargs
def checkargs_dict(a_dict: dict):
    return isinstance(a_dict, dict)


@checkargs
def checkargs_tuple(a_tuple: tuple):
    return isinstance(a_tuple, tuple)


@checkargs
def checkargs_function(a_function: Callable):
    return isinstance(a_function, Callable)


class ReturnBytes:
    """
    This class returns a bytes objects rather than a string when str() is called on it
    """

    def __str__(self):
        return b""


class MockTimeGenerator:
    """
    This class mocks the in-built Python 'time' module and allows you to return a time that you specify
    upon creation of the instance of the class.
    """

    def __init__(self, current_datetime):
        """
        :param int current_datetime: The current datetime in seconds since 1970
        """
        self.current_datetime = current_datetime

    def time(self):
        """

        :return: int datetime current_datetime: The current datetime in seconds since 1970
        """
        return self.current_datetime


class MockTimeGeneratorWrongReturnType:
    """
    This class mocks the in-built Python 'time' module and allows you to return a time that you specify
    upon creation of the instance of the class.
    """

    def __init__(self, current_datetime):
        """
        :param int current_datetime: The current datetime in seconds since 1970
        """
        self.current_datetime = current_datetime

    def time(self):
        """

        :return: int datetime current_datetime: The current datetime in seconds since 1970
        """
        return str(self.current_datetime)


class MockTimeGeneratorNoTimeMethod:
    """
    This class mocks the in-built Python 'time' module and allows you to return a time that you specify
    upon creation of the instance of the class.
    """

    def __init__(self, current_datetime):
        """
        :param int current_datetime: The current datetime in seconds since 1970
        """
        self.time = current_datetime


class CocoonUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            [
                ["tax_lots", "cost", "amount"],
                "CostBaseValue",
                {"tax_lots": {"cost": {"amount": "CostBaseValue"}}},
            ],
            [
                ["tax_lots", "cost", "price"],
                "CostAveragePrice",
                {"tax_lots": {"cost": {"price": "CostAveragePrice"}}},
            ],
        ]
    )
    def test_expand_dictionary_single_recursive(
        self, key_list, value, expected_outcome
    ) -> None:
        """
        Tests that the recursive function to create a nested dictionary from a list of keys and a final value
        provides the expected outcome

        :param list[str] key_list: The list of keys to expand into a nested dictionary
        :param str value: The value to use against the last key
        :param dict expected_outcome: The expected nested dictionary
        :return: None
        """

        nested_dictionary = cocoon.utilities.expand_dictionary_single_recursive(
            index=0, key_list=key_list, value=value
        )

        self.assertTrue(
            expr=all(
                value == nested_dictionary[key]
                for key, value in expected_outcome.items()
            ),
            msg="The expansion of a list of keys into a nested dictionary does not match the expected outcome",
        )

    @parameterized.expand(
        [
            [
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "tax_lots": {
                        "cost": {"amount": None, "currency": "Local Currency Code"},
                        "portfolio_cost": None,
                        "price": None,
                        "purchase_date": None,
                        "settlement_date": None,
                    }
                },
            ]
        ]
    )
    def test_expand_dictionary(self, compacted_dictionary, expected_outcome) -> None:
        """
        Tests that the expansion of a dictionary returns the expected result

        :param: dict compacted_dictionary: The compacted dictionary with keys separated by '.' to be expanded
        :param: dict expected_outcome: The expected expanded, nested dictionary

        :return: None
        """

        expanded_dictionary = cocoon.utilities.expand_dictionary(
            dictionary=compacted_dictionary
        )

        self.assertTrue(
            expr=all(
                value == expanded_dictionary[key]
                for key, value in expected_outcome.items()
            ),
            msg="The expanded dictionary does not match the expected outcome",
        )

    @parameterized.expand(
        [
            [
                {
                    "portfolio_code": "FundCode",
                    "effective_date": "Effective Date",
                    "tax_lots": {"units": "Quantity"},
                },
                {
                    "tax_lots": {
                        "cost": {"amount": None, "currency": "Local Currency Code"},
                        "portfolio_cost": None,
                        "price": None,
                        "purchase_date": None,
                        "settlement_date": None,
                    }
                },
                {
                    "portfolio_code": "FundCode",
                    "effective_date": "Effective Date",
                    "tax_lots": {
                        "cost": {"amount": None, "currency": "Local Currency Code"},
                        "units": "Quantity",
                        "portfolio_cost": None,
                        "price": None,
                        "purchase_date": None,
                        "settlement_date": None,
                    },
                },
            ]
        ]
    )
    def test_update_nested_dictionary(
        self, nested_dictionary_1, nested_dictionary_2, expected_outcome
    ) -> None:
        """
        Tests that updating a nested dictionary provides the correct outcome

        :param dict nested_dictionary_1: The original nested dictionary
        :param dict nested_dictionary_2: The new nested dictionary
        :param dict expected_outcome: The expected updated nested dictionary from updating the original with the new

        :return: None
        """

        cocoon.utilities.update_dict(
            orig_dict=nested_dictionary_1, new_dict=nested_dictionary_2
        )

        for key, value in expected_outcome.items():
            self.assertEqual(
                first=value,
                second=nested_dictionary_1[key],
                msg="The key of a nested dictionary does not match the expected outcome",
            )

    @parameterized.expand(
        [
            # Test building an InstrumentDefinition
            [
                lusid.models.InstrumentDefinition,
                {
                    "Instrument/CreditRatings/Moodys": lusid.models.PerpetualProperty(
                        key="Instrument/CreditRatings/Moodys",
                        value=lusid.models.PropertyValue(label_value="A2"),
                    ),
                    "Instrument/CreditRatings/SandP": lusid.models.PerpetualProperty(
                        key="Instrument/CreditRatings/SandP",
                        value=lusid.models.PropertyValue(label_value="A-"),
                    ),
                },
                {
                    "Instrument/default/Figi": lusid.models.InstrumentIdValue(
                        value="BBG000BLNNH6"
                    ),
                    "Instrument/default/Ticker": lusid.models.InstrumentIdValue(
                        value="IBM"
                    ),
                },
                [],
                {
                    "name": "instrument_name",
                    "look_through_portfolio_id": {
                        "scope": "lookthrough_scope",
                        "code": "lookthrough_code",
                    },
                    "definition": {
                        "instrument_format": "format",
                        "content": "contract",
                    },
                },
                pd.Series(
                    data=[
                        "GlobalCreditFund",
                        "SingaporeBranch",
                        "PORT_12490FKS9",
                        "json",
                        "{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                    ],
                    index=[
                        "instrument_name",
                        "lookthrough_scope",
                        "lookthrough_code",
                        "format",
                        "contract",
                    ],
                ),
                lusid.models.InstrumentDefinition(
                    name="GlobalCreditFund",
                    identifiers={
                        "Instrument/default/Figi": lusid.models.InstrumentIdValue(
                            value="BBG000BLNNH6"
                        ),
                        "Instrument/default/Ticker": lusid.models.InstrumentIdValue(
                            value="IBM"
                        ),
                    },
                    properties={
                        "Instrument/CreditRatings/Moodys": lusid.models.PerpetualProperty(
                            key="Instrument/CreditRatings/Moodys",
                            value=lusid.models.PropertyValue(label_value="A2"),
                        ),
                        "Instrument/CreditRatings/SandP": lusid.models.PerpetualProperty(
                            key="Instrument/CreditRatings/SandP",
                            value=lusid.models.PropertyValue(label_value="A-"),
                        ),
                    },
                    look_through_portfolio_id=lusid.models.ResourceId(
                        scope="SingaporeBranch", code="PORT_12490FKS9"
                    ),
                    definition=lusid.models.InstrumentEconomicDefinition(
                        instrument_format="json",
                        content="{asset_type: 'fund', term_sheet: 'GlobalCreditFundConstituents.pdf'}",
                    ),
                ),
            ],
            # Test building a CreateTransactionPortfolioRequest
            [
                lusid.models.CreateTransactionPortfolioRequest,
                {
                    "Portfolio/Manager/Id": lusid.models.PerpetualProperty(
                        key="Portfolio/Manager/Id",
                        value=lusid.models.PropertyValue(label_value="PM_1241247"),
                    ),
                    "Portfolio/Operations/Rebalancing_Interval": lusid.models.PerpetualProperty(
                        key="Portfolio/Operations/Rebalancing_Interval",
                        value=lusid.models.PropertyValue(
                            metric_value=lusid.models.MetricValue(value=30, unit="Days")
                        ),
                    ),
                },
                None,
                ["Transaction/Operations/Strategy_Tag"],
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                    "description": "description",
                    "accounting_method": "accounting_method",
                    "corporate_action_source_id": {"scope": None, "code": None},
                },
                pd.Series(
                    data=[
                        "PORT_42424",
                        "GlobalCreditFundPortfolio",
                        datetime(year=2019, month=10, day=5, tzinfo=pytz.UTC),
                        "GBP",
                        "Global Credit Fund Portfolio",
                        "AverageCost",
                    ],
                    index=[
                        "FundCode",
                        "display_name",
                        "created",
                        "base_currency",
                        "description",
                        "accounting_method",
                    ],
                ),
                lusid.models.CreateTransactionPortfolioRequest(
                    display_name="GlobalCreditFundPortfolio",
                    description="Global Credit Fund Portfolio",
                    code="PORT_42424",
                    created=datetime(
                        year=2019, month=10, day=5, tzinfo=pytz.UTC
                    ).isoformat(),
                    base_currency="GBP",
                    corporate_action_source_id=None,
                    accounting_method="AverageCost",
                    sub_holding_keys=["Transaction/Operations/Strategy_Tag"],
                    properties={
                        "Portfolio/Manager/Id": lusid.models.PerpetualProperty(
                            key="Portfolio/Manager/Id",
                            value=lusid.models.PropertyValue(label_value="PM_1241247"),
                        ),
                        "Portfolio/Operations/Rebalancing_Interval": lusid.models.PerpetualProperty(
                            key="Portfolio/Operations/Rebalancing_Interval",
                            value=lusid.models.PropertyValue(
                                metric_value=lusid.models.MetricValue(
                                    value=30, unit="Days"
                                )
                            ),
                        ),
                    },
                ),
            ],
            # Test building a TransactionRequest
            [
                lusid.models.TransactionRequest,
                {
                    "Transaction/Operations/Strategy_Tag": lusid.models.PerpetualProperty(
                        key="Transaction/Operations/Strategy_Tag",
                        value=lusid.models.PropertyValue(
                            label_value="QuantitativeSignal"
                        ),
                    ),
                    "Transaction/Operations/Cash_Account": lusid.models.PerpetualProperty(
                        key="Transaction/Operations/Accrued_Interest",
                        value=lusid.models.PropertyValue(
                            metric_value=lusid.models.MetricValue(
                                value=30.52, unit="GBP"
                            )
                        ),
                    ),
                },
                {
                    "Instrument/default/Figi": "BBG000BLNNH6",
                    "Instrument/default/Ticker": "IBM",
                },
                [],
                {
                    "portfolio_code": "FundCode",
                    "transaction_id": "transaction_id",
                    "type": "transaction_type",
                    "transaction_date": "Effective Date",
                    "settlement_date": "Effective Date",
                    "units": "Quantity",
                    "transaction_price": {"price": "Local Price", "type": None},
                    "total_consideration": {
                        "amount": "Local Market Value",
                        "currency": "Local Currency Code",
                    },
                    "transaction_currency": "Local Currency Code",
                    "exchange_rate": "exchange_rate",
                    "source": None,
                    "counterparty_id": None,
                },
                pd.Series(
                    data=[
                        "PORT_42424",
                        "TID_98391235",
                        "Buy",
                        datetime(year=2019, month=9, day=3, tzinfo=pytz.UTC),
                        100000,
                        15,
                        1500000,
                        "USD",
                        1.2,
                    ],
                    index=[
                        "FundCode",
                        "transaction_id",
                        "transaction_type",
                        "Effective Date",
                        "Quantity",
                        "Local Price",
                        "Local Market Value",
                        "Local Currency Code",
                        "exchange_rate",
                    ],
                ),
                lusid.models.TransactionRequest(
                    transaction_id="TID_98391235",
                    type="Buy",
                    instrument_identifiers={
                        "Instrument/default/Figi": "BBG000BLNNH6",
                        "Instrument/default/Ticker": "IBM",
                    },
                    transaction_date="2019-09-03T00:00:00+00:00",
                    settlement_date="2019-09-03T00:00:00+00:00",
                    units=100000,
                    transaction_price=lusid.models.TransactionPrice(
                        price=15, type=None
                    ),
                    total_consideration=lusid.models.CurrencyAndAmount(
                        amount=1500000, currency="USD"
                    ),
                    exchange_rate=1.2,
                    transaction_currency="USD",
                    properties={
                        "Transaction/Operations/Strategy_Tag": lusid.models.PerpetualProperty(
                            key="Transaction/Operations/Strategy_Tag",
                            value=lusid.models.PropertyValue(
                                label_value="QuantitativeSignal"
                            ),
                        ),
                        "Transaction/Operations/Cash_Account": lusid.models.PerpetualProperty(
                            key="Transaction/Operations/Accrued_Interest",
                            value=lusid.models.PropertyValue(
                                metric_value=lusid.models.MetricValue(
                                    value=30.52, unit="GBP"
                                )
                            ),
                        ),
                    },
                    source=None,
                ),
            ],
            # Test building an AdjustHoldingRequest
            [
                lusid.models.AdjustHoldingRequest,
                {
                    "Holding/Operations/MarketDataVendor": lusid.models.PerpetualProperty(
                        key="Holding/Operations/MarketDataVendor",
                        value=lusid.models.PropertyValue(label_value="ipsum_lorem"),
                    ),
                    "Holding/Operations/MarketValBaseCurrency": lusid.models.PerpetualProperty(
                        key="Holding/Operations/MarketValBaseCurrency",
                        value=lusid.models.PropertyValue(
                            metric_value=lusid.models.MetricValue(
                                value=4567002.43, unit="GBP"
                            )
                        ),
                    ),
                },
                {
                    "Instrument/default/Figi": "BBG000BLNNH6",
                    "Instrument/default/Ticker": "IBM",
                },
                {
                    "Transaction/Operations/Strategy_Tag": lusid.models.PerpetualProperty(
                        key="Transaction/Operations/Strategy_Tag",
                        value=lusid.models.PropertyValue(
                            label_value="QuantitativeSignal"
                        ),
                    ),
                    "Transaction/Operations/Cash_Account": lusid.models.PerpetualProperty(
                        key="Transaction/Operations/Accrued_Interest",
                        value=lusid.models.PropertyValue(
                            metric_value=lusid.models.MetricValue(
                                value=30.52, unit="GBP"
                            )
                        ),
                    ),
                },
                {
                    "tax_lots": {
                        "cost": {"amount": None, "currency": "Local Currency Code"},
                        "portfolio_cost": None,
                        "price": None,
                        "purchase_date": None,
                        "settlement_date": None,
                        "units": "QTY",
                    }
                },
                pd.Series(data=["GBP", 10000], index=["Local Currency Code", "QTY"]),
                lusid.models.AdjustHoldingRequest(
                    instrument_identifiers={
                        "Instrument/default/Figi": "BBG000BLNNH6",
                        "Instrument/default/Ticker": "IBM",
                    },
                    properties={
                        "Holding/Operations/MarketDataVendor": lusid.models.PerpetualProperty(
                            key="Holding/Operations/MarketDataVendor",
                            value=lusid.models.PropertyValue(label_value="ipsum_lorem"),
                        ),
                        "Holding/Operations/MarketValBaseCurrency": lusid.models.PerpetualProperty(
                            key="Holding/Operations/MarketValBaseCurrency",
                            value=lusid.models.PropertyValue(
                                metric_value=lusid.models.MetricValue(
                                    value=4567002.43, unit="GBP"
                                )
                            ),
                        ),
                    },
                    sub_holding_keys={
                        "Transaction/Operations/Strategy_Tag": lusid.models.PerpetualProperty(
                            key="Transaction/Operations/Strategy_Tag",
                            value=lusid.models.PropertyValue(
                                label_value="QuantitativeSignal"
                            ),
                        ),
                        "Transaction/Operations/Cash_Account": lusid.models.PerpetualProperty(
                            key="Transaction/Operations/Accrued_Interest",
                            value=lusid.models.PropertyValue(
                                metric_value=lusid.models.MetricValue(
                                    value=30.52, unit="GBP"
                                )
                            ),
                        ),
                    },
                    tax_lots=[
                        lusid.models.TargetTaxLotRequest(
                            units=10000,
                            cost=lusid.models.CurrencyAndAmount(currency="GBP"),
                        )
                    ],
                ),
            ],
        ]
    )
    def test_set_attributes(
        self,
        model_object,
        properties,
        identifiers,
        sub_holding_keys,
        mapping,
        row,
        expected_outcome,
    ) -> None:
        """
        Tests that setting the attributes on a model works as expected
        :param lusid.models model_object: The class of the model object to populate
        :param any properties: The properties to use on this model
        :param any identifiers: The instrument identifiers to use on this
        :param list[str] sub_holding_keys: The sub holding keys to use on this model
        :param dict mapping: The expanded dictionary mapping the Series columns to the LUSID model attributes
        :param pd.Series row: The current row of the DataFrame being worked on
        :param lusid.models expected_outcome: An instance of the model object with populated attributes

        :return: None
        """

        populated_model = cocoon.utilities.set_attributes_recursive(
            model_object=model_object,
            mapping=mapping,
            row=row,
            properties=properties,
            identifiers=identifiers,
            sub_holding_keys=sub_holding_keys,
        )

        self.assertEqual(
            first=populated_model,
            second=expected_outcome,
            msg="The populated model does not match the expected outcome",
        )

    def test_populate_model(self):
        pass

    def test_file_type_checks(self):
        pass

    @parameterized.expand(
        [
            [
                "Explicitly known invalid character '&'",
                "S&PCreditRating(UK)",
                "SandPCreditRatingUK",
            ],
            ["Explicitly known invalid character '%'", "Return%", "ReturnPercentage"],
            [
                "Explicitly known invalid character '.'",
                "balances.available",
                "balances_available",
            ],
            [
                "Invalid character not meeting regex #1 - /",
                "Buy/Sell Indicator",
                "BuySellIndicator",
            ],
            [
                "Invalid character not meeting regex #2 - £$",
                "£DollarDollarBills$",
                "DollarDollarBills",
            ],
            [
                "Invalid character not meeting regex #3 - Space",
                "Dollar Dollar Bills",
                "DollarDollarBills",
            ],
            ["Integer", 1, "1"],
            ["Decimal", 1.8596, "1_8596"],
            ["List", ["My", "List", "Code"], "MyListCode"],
        ]
    )
    def test_make_code_lusid_friendly_success(
        self, _, enemy_code, expected_code
    ) -> None:
        """
        This tests that the utility to make codes LUSID friendly works as expected

        :param str enemy_code: The unfriendly (enemy) code to convert to a LUSID friendly code
        :param str expected_code: The expected LUSID friendly code after conversion

        :return: None
        """

        friendly_code = cocoon.utilities.make_code_lusid_friendly(raw_code=enemy_code)

        self.assertEqual(
            first=friendly_code,
            second=expected_code,
            msg=f"The friendly code '{friendly_code}'' does not match the expected output '{expected_code}'",
        )

    @parameterized.expand(
        [
            [
                "Code exceeds character limit",
                "S&PCreditRating(UK)ThisIsAReallyLongCodeThatExceedsTheCharacterLimit",
                ValueError,
            ],
            ["Code cannot be converted to a string", ReturnBytes(), Exception,],
        ]
    )
    def test_make_code_lusid_friendly_failure(
        self, _, enemy_code, expected_exception
    ) -> None:
        """
        This tests that the utility to make codes LUSID friendly works as expected

        :param str enemy_code: The unfriendly (enemy) code to convert to a LUSID friendly code
        :param expected_exception: The expected exception

        :return: None
        """

        with self.assertRaises(expected_exception):
            cocoon.utilities.make_code_lusid_friendly(raw_code=enemy_code)

    @parameterized.expand(
        [
            [
                {
                    "definitions": {
                        "InstrumentDefinition": {
                            "required": ["name", "identifiers"],
                            "type": "object",
                            "properties": {
                                "name": {
                                    "description": "The name of the instrument.",
                                    "type": "string",
                                },
                                "identifiers": {
                                    "description": "A set of identifiers that can be used to identify the instrument. At least one of these must be configured to be a unique identifier.",
                                    "type": "object",
                                    "additionalProperties": {
                                        "$ref": "#/definitions/InstrumentIdValue"
                                    },
                                },
                                "properties": {
                                    "description": "Set of unique instrument properties and associated values to store with the instrument. Each property must be from the 'Instrument' domain.",
                                    "uniqueItems": "false",
                                    "type": "array",
                                    "items": {"$ref": "#/definitions/Property"},
                                },
                                "lookThroughPortfolioId": {
                                    "$ref": "#/definitions/ResourceId",
                                    "description": "The identifier of the portfolio that has been securitised to create this instrument.",
                                },
                                "definition": {
                                    "$ref": "#/definitions/InstrumentEconomicDefinition",
                                    "description": "The economic definition of the instrument where an expanded definition exists. in the case of OTC instruments this contains the definition of the non-exchange traded instrument. There is no validation on the structure of this definition. However, in order to transparently use vendor libraries it must conform to a format that LUSID understands.",
                                },
                            },
                        },
                        "InstrumentIdValue": {
                            "required": ["value"],
                            "type": "object",
                            "properties": {
                                "value": {
                                    "description": "The value of the identifier.",
                                    "type": "string",
                                },
                                "effectiveAt": {
                                    "format": "date-time",
                                    "description": "The effective datetime from which the identifier will be valid. If left unspecified the default value is the beginning of time.",
                                    "type": "string",
                                },
                            },
                        },
                    }
                },
                lusid.models.InstrumentDefinition,
                ["name", "identifiers.value"],
            ]
        ]
    )
    def test_get_required_attributes_model_recursive(
        self, swagger_dict, model_object, expected_outcome
    ):
        required_attributes = cocoon.utilities.get_required_attributes_model_recursive(
            model_object=model_object
        )

        required_attributes.sort()
        expected_outcome.sort()

        self.assertListEqual(required_attributes, expected_outcome)

    def test_gen_dict_extract(self):
        pass

    @parameterized.expand(
        [
            ["Test on ResourceId where the model does exist", "ResourceId", True,],
            ["Test where it is a string does not exist at all", "str", False],
            [
                "Test where it is inside a dictionary",
                "dict(str, InstrumentIdValue)",
                True,
            ],
            ["Test where it is inside a list", "list[ModelProperty]", True],
        ]
    )
    def test_check_nested_model(
        self, _, required_attribute_properties, expected_outcome
    ) -> None:
        """
        Tests that the name of a nested model is successfully returned from the properties of a required attribute

        :param dict required_attribute_properties:
        :param string expected_outcome:

        :return: None
        """
        nested_model_check = cocoon.utilities.check_nested_model(
            required_attribute_properties
        )

        self.assertEqual(first=expected_outcome, second=nested_model_check)

    @parameterized.expand(
        [
            # Test all required attributes exist
            [{"name": "instrument_name"}, lusid.models.InstrumentDefinition.__name__],
            # Test all required attributes exist as well as an extra attribute
            [
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                    "portfolio_type": "account_type",
                },
                lusid.models.CreateTransactionPortfolioRequest.__name__,
            ],
            # Test all required attributes exist, plus an exempt attribute
            [
                {"name": "instrument_name", "identifiers": "instrument_internal"},
                lusid.models.InstrumentDefinition.__name__,
            ],
        ]
    )
    def test_verify_all_required_attributes_mapped_success(
        self, mapping, model_object
    ) -> None:
        """
        Tests that you can successfully verify that all required attributes exist

        :param dict mapping: The required mapping dictionary
        :param lusid.model model_object: The model object to verify against

        :return: None
        """

        cocoon.utilities.verify_all_required_attributes_mapped(
            mapping=mapping,
            model_object_name=model_object,
            exempt_attributes=[
                "identifiers",
                "properties",
                "instrument_identifiers",
                "sub_holding_keys",
            ],
        )

    @parameterized.expand(
        [
            # Test no required attributes provided via an empty dictionary
            [{}, lusid.models.InstrumentDefinition, ValueError],
            # Test no required attributes required via None provided
            [None, lusid.models.InstrumentDefinition, TypeError],
            # Test a missing required attribute
            [
                {
                    "portfolio_code": "portfolio_code",
                    "transaction_type": "transaction_type",
                    "transaction_date": "transaction_date",
                    "settlement_date": "settlement_date",
                    "units": "units",
                    "transaction_price.price": "transaction_price",
                    "total_consideration.amount": "amount",
                    "total_consideration.currency": "trade_currency",
                },
                lusid.models.TransactionRequest,
                ValueError,
            ],
        ]
    )
    def test_verify_all_required_attributes_mapped_fail(
        self, mapping, model_object, expected_exception
    ) -> None:
        """
        Test that an exception on failure is successfully raised

        :param dict mapping: The required mapping dictionary
        :param lusid.model model_object: The model object to verify against
        :param expected_exception: The expected exception

        :return: None
        """

        with self.assertRaises(expected_exception):
            cocoon.utilities.verify_all_required_attributes_mapped(
                mapping=mapping,
                model_object=model_object,
                exempt_attributes=[
                    "identifiers",
                    "properties",
                    "instrument_identifiers",
                    "sub_holding_keys",
                ],
            )

    @parameterized.expand(
        [["instrumentIdentifiers", "instrument_identifiers"], ["taxLots", "tax_lots"]]
    )
    def test_camel_case_to_pep_8(self, attribute_name, expected_outcome) -> None:
        """
        Tests that the conversion from camel case e.g. instrumentIdentifiers is converted to Python PEP 8 standard
        i.e. instrument_identifiers

        :param str attribute_name: The attribute name to convert
        :param str expected_outcome: The expected outcome

        :return: None
        """

        pep_8_name = cocoon.utilities.camel_case_to_pep_8(attribute_name=attribute_name)

        self.assertEqual(first=pep_8_name, second=expected_outcome)

    @parameterized.expand(
        [
            [
                ["Travel", "Airlines and Aviation Services"],
                "Travel, Airlines and Aviation Services",
            ],
            [10, 10],
            [
                {"Travel": "Airlines and Aviation Services"},
                "{'Travel': 'Airlines and Aviation Services'}",
            ],
            ["Rebalanced", "Rebalanced"],
        ]
    )
    def test_convert_cell_value_to_string(self, data, expected_outcome) -> None:
        """
        Tests that data can be successfully converted to a string if it is a list or a dictionary

        :param data: The data input
        :param expected_outcome: The expected result

        :return: None
        """
        converted_value = cocoon.utilities.convert_cell_value_to_string(data)

        self.assertEqual(first=converted_value, second=expected_outcome)

    @parameterized.expand(
        [
            # Test a column and default
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": {"column": "instrument_name", "default": "unknown_name"}},
                [
                    pd.DataFrame(
                        data={
                            "instrument_name": [
                                "International Business Machines",
                                "unknown_name",
                                "Amazon",
                                "Apple",
                            ]
                        }
                    ),
                    {"name": "instrument_name"},
                ],
            ],
            # Test just a column
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": {"column": "instrument_name",}},
                [
                    pd.DataFrame(
                        data={
                            "instrument_name": [
                                "International Business Machines",
                                np.NaN,
                                "Amazon",
                                "Apple",
                            ]
                        }
                    ),
                    {"name": "instrument_name"},
                ],
            ],
            # Test just a default
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": {"default": "unknown_name",}},
                [
                    pd.DataFrame(
                        data={
                            "instrument_name": [
                                "International Business Machines",
                                np.NaN,
                                "Amazon",
                                "Apple",
                            ],
                            "LUSID.name": [
                                "unknown_name",
                                "unknown_name",
                                "unknown_name",
                                "unknown_name",
                            ],
                        }
                    ),
                    {"name": "LUSID.name"},
                ],
            ],
            # Test no nesting
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": "instrument_name"},
                [
                    pd.DataFrame(
                        data={
                            "instrument_name": [
                                "International Business Machines",
                                np.NaN,
                                "Amazon",
                                "Apple",
                            ]
                        }
                    ),
                    {"name": "instrument_name"},
                ],
            ],
        ]
    )
    def test_handle_nested_default_and_column_mapping_success(
        self, data_frame, mapping, expected_outcome
    ):
        (
            updated_data_frame,
            updated_mapping,
        ) = cocoon.utilities.handle_nested_default_and_column_mapping(
            data_frame=data_frame, mapping=mapping
        )

        self.assertTrue(expr=updated_data_frame.equals(expected_outcome[0]))

        self.assertEqual(first=updated_mapping, second=expected_outcome[1])

    @parameterized.expand(
        [
            # Test providing a dictionary with no column or default keys
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": {"columns": "instrument_name", "defaults": "unknown_name"}},
                KeyError,
            ],
            # Test providing an empty dictionary
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": {}},
                KeyError,
            ],
            # Test providing a list
            [
                pd.DataFrame(
                    data={
                        "instrument_name": [
                            "International Business Machines",
                            np.NaN,
                            "Amazon",
                            "Apple",
                        ]
                    }
                ),
                {"name": ["instrument_name"]},
                ValueError,
            ],
        ]
    )
    def test_handle_nested_default_and_column_mapping_failure(
        self, data_frame, mapping, expected_exception
    ):
        with self.assertRaises(expected_exception):
            cocoon.utilities.handle_nested_default_and_column_mapping(
                data_frame=data_frame, mapping=mapping
            )

    @parameterized.expand(
        [
            # Test properties_scope as a list
            [
                cocoon.cocoon.load_from_data_frame,
                TypeError,
                [
                    lusid.utilities.ApiClientFactory(
                        api_secrets_filename=Path(
                            __file__
                        ).parent.parent.parent.joinpath("secrets.json")
                    ),
                    "test_scope",
                    pd.DataFrame(),
                    {"name": "instrument_name"},
                    {"definition.format": "json"},
                    "instrument",
                ],
                {
                    "identifier_mapping": {},
                    "property_columns": [],
                    "properties_scope": ["help"],
                },
            ],
            # Test with no keyword arguments and a Pandas Series instead of a DataFrame
            [
                cocoon.cocoon.load_from_data_frame,
                TypeError,
                [
                    lusid.utilities.ApiClientFactory(
                        api_secrets_filename=Path(
                            __file__
                        ).parent.parent.parent.joinpath("secrets.json")
                    ),
                    "test_scope",
                    pd.Series(),
                    {"name": "instrument_name"},
                    {"definition.format": "json"},
                    "instrument",
                ],
                {},
            ],
            # Test properties_scope as a list but as the first keyword argument
            [
                cocoon.cocoon.load_from_data_frame,
                TypeError,
                [
                    lusid.utilities.ApiClientFactory(
                        api_secrets_filename=Path(
                            __file__
                        ).parent.parent.parent.joinpath("secrets.json")
                    ),
                    "test_scope",
                    pd.DataFrame(),
                    {"name": "instrument_name"},
                    {"definition.format": "json"},
                    "instrument",
                ],
                {
                    "properties_scope": ["help"],
                    "identifier_mapping": {},
                    "property_columns": [],
                },
            ],
            # Test identifier_mapping as a string but as a positional argument
            [
                cocoon.cocoon.load_from_data_frame,
                TypeError,
                [
                    lusid.utilities.ApiClientFactory(
                        api_secrets_filename=Path(
                            __file__
                        ).parent.parent.parent.joinpath("secrets.json")
                    ),
                    "test_scope",
                    pd.DataFrame(),
                    {"name": "instrument_name"},
                    {"definition.format": "json"},
                    "instrument",
                    "instrument_identifiers: figi",
                ],
                {"properties_scope": ["help"], "property_columns": []},
            ],
            # Test identifier_mapping as None with a properties scope as a list
            [
                cocoon.cocoon.load_from_data_frame,
                TypeError,
                [
                    lusid.utilities.ApiClientFactory(
                        api_secrets_filename=Path(
                            __file__
                        ).parent.parent.parent.joinpath("secrets.json")
                    ),
                    "test_scope",
                    pd.DataFrame(),
                    {"name": "instrument_name"},
                    {"definition.format": "json"},
                    "instrument",
                    None,
                ],
                {"properties_scope": ["help"], "property_columns": []},
            ],
        ]
    )
    def test_checkargs_lusid(self, function, expected_exception, args, kwargs):
        with self.assertRaises(expected_exception):
            function(*args, **kwargs)

    @parameterized.expand(
        [
            ("list_string", checkargs_list, ["a", "b", "c"]),
            ("list_number", checkargs_list, [1, 2, 3]),
            ("dict_string", checkargs_dict, {"a": "b"}),
            ("dict_mixed", checkargs_dict, {"a": 1}),
            ("dict_number", checkargs_dict, {1: 2}),
            ("function", checkargs_function, lambda: print("lambda")),
        ]
    )
    def test_checkargs(self, _, function, param):
        self.assertTrue(function(param))

    @parameterized.expand(
        [
            ("list_string", checkargs_list, {}),
            ("list_none", checkargs_list, None),
            ("dict_string", checkargs_dict, "a"),
            ("dict_string", checkargs_dict, None),
            ("function", checkargs_function, []),
            ("function", checkargs_function, None),
        ]
    )
    def test_checkargs_with_incorrect_type(self, _, function, param):
        with self.assertRaises(TypeError):
            function(param)

    @parameterized.expand([["list_string", checkargs_list, {"b_list": []}]])
    def test_checkargs_with_invalid_argument(self, _, function, kwargs):
        with self.assertRaises(ValueError):
            function(**kwargs)

    @parameterized.expand(
        [
            (
                "only scale bonds",
                [["name1", "s", 100.0], ["name2", "s", 100.0], ["name3", "b", 10000.0]],
                [100, 100, 100],
                0.01,
            ),
            (
                "missing non-type values",
                [["name1", "s", 100.0], ["name2", "s", None], ["name3", "b", 1000.0]],
                [100, None, 100.0],
                0.1,
            ),
            (
                "missing type values",
                [["name1", "s", 100.0], ["name2", "s", 100.0], ["name3", "b", None]],
                [100, 100.0, None],
                0.1,
            ),
        ]
    )
    def test_scale_quote_of_type(self, _, data, ground_truth, scale_factor):
        df = pd.DataFrame(data, columns=["name", "type", "price"])
        mapping = {
            "quotes": {
                "quote_scalar": {
                    "price": "price",
                    "type": "type",
                    "type_code": "b",
                    "scale_factor": scale_factor,
                },
                "required": {
                    "quote_id.quote_series_id.instrument_id_type": "$Isin",
                    "quote_id.effective_at": "date",
                    "quote_id.quote_series_id.provider": "$DataScope",
                    "quote_id.quote_series_id.field": "$mid",
                    "quote_id.quote_series_id.quote_type": "$Price",
                    "quote_id.quote_series_id.instrument_id": "isin",
                    "metric_value.unit": "currency",
                    "metric_value.value": "price",
                },
            }
        }
        result, mapping = cocoon.utilities.scale_quote_of_type(df=df, mapping=mapping)

        [
            self.assertEqual(ground_truth[index], row["__adjusted_quote"])
            for index, row in result.iterrows()
        ]

        self.assertEqual(
            "__adjusted_quote", mapping["quotes"]["required"]["metric_value.value"]
        )

    @parameterized.expand(
        [
            ("invalid_type_column", "invalid_type_name", "type", KeyError),
            ("invalid_price_column", "invalid_price_name", "price", KeyError),
        ]
    )
    def test_scale_quote_of_type_fail(self, _, col_title, column, error_type):
        df = pd.DataFrame(
            [["name1", "s", 100.0], ["name2", "s", 100.0], ["name3", "b", 10000.0]],
            columns=["name", "type", "price"],
        )
        mapping = {
            "quotes": {
                "quote_scalar": {
                    "price": "price",
                    "type": "type",
                    "type_code": "b",
                    "scale_factor": 0.01,
                }
            }
        }
        mapping["quotes"]["quote_scalar"][column] = col_title
        with self.assertRaises(error_type):
            cocoon.utilities.scale_quote_of_type(df=df, mapping=mapping)

    @parameterized.expand(
        [
            ("comma", ","),
            ("vertical bar", "|"),
            ("percent", "%"),
            ("ampersand", "&"),
            ("backslash", "/"),
            ("tilde", "~"),
            ("asterisk", "*"),
            ("hash", "#"),
            ("tab", "{}".format("\t")),
        ]
    )
    def test_get_delimiter(self, _, delimiter):
        sample_string = [f"data{i}" + delimiter for i in range(10)]
        sample_string = "".join(sample_string)
        delimiter_detected = get_delimiter(sample_string)
        self.assertEqual(delimiter, delimiter_detected)

    def test_check_mapping_fields_exist(self):
        required_list = ["field1", "field4", "field6"]
        search_list = ["field1", "field2", "field3", "field4", "field5", "field6"]
        self.assertFalse(
            check_mapping_fields_exist(required_list, search_list, "test_file_type")
        )

    def test_check_mapping_fields_exist_fail(self):
        required_list = ["field1", "field4", "field7", "field8"]
        search_list = ["field1", "field2", "field3", "field4", "field5", "field6"]

        with self.assertRaises(ValueError):
            check_mapping_fields_exist(required_list, search_list, "test_file_type")

    @parameterized.expand(
        [
            (
                "implicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_EXP", "GBP_EXP", "USD_EXP", "USD_EXP"],
            ),
            (
                "explicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                False,
                ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP"],
            ),
            (
                "combined_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "",
                        },
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_EXP"],
            ),
            (
                "implicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                False,
                [],
            ),
            (
                "explicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                False,
                [],
            ),
            (
                "combined_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    },
                    "implicit": "internal_currency",
                },
                False,
                [],
            ),
        ]
    )
    def test_identify_cash_items_failed(
        self, _, cash_flag, remove_cash_items, ground_truth
    ):
        data = {
            "instrument_name": ["inst1", "inst2", "inst3", "inst4", "inst5"],
            "internal_currency": ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP", "APPLUK"],
            "Figi": ["BBG01", None, None, None, "BBG02"],
        }
        file_type = "instruments"
        identifier_mapping = {"Figi": "figi"}
        ground_truth.append(None)
        mappings = {
            file_type: {"identifier_mapping": identifier_mapping},
            "cash_flag": cash_flag,
        }
        mappings_ground_truth = copy.deepcopy(mappings)

        if not remove_cash_items:
            mappings_ground_truth[file_type]["identifier_mapping"][
                "Currency"
            ] = "__currency_identifier_for_LUSID"

        dataframe = pd.DataFrame(data)

        dataframe, mappings_test = identify_cash_items(
            dataframe, mappings, file_type, remove_cash_items
        )

        with self.assertRaises(AssertionError):
            if remove_cash_items:
                self.assertEqual(
                    1, len(list(dataframe["__currency_identifier_for_LUSID"]))
                )
            else:
                self.assertEqual(
                    4, len(list(dataframe["__currency_identifier_for_LUSID"]))
                )
            self.assertEqual(
                ground_truth, list(dataframe["__currency_identifier_for_LUSID"])
            )
            self.assertEqual(mappings_ground_truth, mappings_test)

    @parameterized.expand(
        [
            (
                "implicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP"],
            ),
            (
                "explicit_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                False,
                ["GBP_EXP", "GBP_EXP", "USD_EXP", "USD_EXP"],
            ),
            (
                "combined_currency_code_inference",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "",
                        },
                    },
                    "implicit": "internal_currency",
                },
                False,
                ["GBP_EXP", "GBP_EXP", "USD_EXP", "USD_IMP"],
            ),
            (
                "implicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": ["inst1", "inst2", "inst3", "inst4"],
                    },
                    "implicit": "internal_currency",
                },
                True,
                [],
            ),
            (
                "explicit_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    }
                },
                True,
                [],
            ),
            (
                "combined_currency_code_inference_and_remove",
                {
                    "cash_identifiers": {
                        "instrument_name": {
                            "inst1": "GBP_EXP",
                            "inst2": "GBP_EXP",
                            "inst3": "USD_EXP",
                            "inst4": "USD_EXP",
                        },
                    },
                    "implicit": "internal_currency",
                },
                True,
                [],
            ),
        ]
    )
    def test_identify_cash_items(self, _, cash_flag, remove_cash_items, ground_truth):
        data = {
            "instrument_name": ["inst1", "inst2", "inst3", "inst4", "inst5"],
            "internal_currency": ["GBP_IMP", "GBP_IMP", "USD_IMP", "USD_IMP", "APPLUK"],
            "Figi": ["BBG01", None, None, None, "BBG02"],
        }
        file_type = "instruments"
        identifier_mapping = {"Figi": "figi"}
        ground_truth.append(None)
        mappings = {
            file_type: {"identifier_mapping": identifier_mapping,},
            "cash_flag": cash_flag,
        }
        mappings_ground_truth = copy.deepcopy(mappings)

        if not remove_cash_items:
            mappings_ground_truth[file_type]["identifier_mapping"][
                "Currency"
            ] = "__currency_identifier_for_LUSID"

        dataframe = pd.DataFrame(data)

        dataframe, mappings_test = identify_cash_items(
            dataframe, mappings, file_type, remove_cash_items
        )

        self.assertEqual(
            ground_truth, list(dataframe["__currency_identifier_for_LUSID"])
        )
        self.assertEqual(mappings_ground_truth, mappings_test)

    @parameterized.expand(
        [
            ("type_string", "GBP", "USD"),
            ("type_float", float(10.10), float(20.20)),
            ("type_bool", True, False),
            ("type int", int(10), int(12)),
            ("type_date", datetime.now(), datetime(2017, 6, 22)),
        ]
    )
    def test_strip_whitespace(self, _, val_1, val_2):
        df_true = pd.DataFrame(
            [["GBP", val_1, "GBP  USD"], ["GBP", "GBP", "GBP"], ["GBP", val_2, "GBP"]],
            columns=["a", "b", "c"],
        )
        df_test = pd.DataFrame(
            [
                ["GBP   ", val_1, "GBP  USD"],
                ["   GBP  ", "   GBP", "GBP"],
                ["GBP   ", val_2, "GBP   "],
            ],
            columns=["a", "b", "c"],
        )
        cols = ["a", "b", "c"]
        df_test = strip_whitespace(df_test, cols)

        self.assertTrue(df_true.equals(df_test))

    @parameterized.expand(
        [
            [
                "2019-11-27T11:08:38Z",
                MockTimeGenerator(current_datetime=1574852918),
                "37f3-342f-823f-00",
            ]
        ]
    )
    def test_create_scope_id_success(self, _, time_generator, expected_outcome):

        scope_id = create_scope_id(time_generator)

        self.assertEqual(first=expected_outcome, second=scope_id)

    @parameterized.expand(
        [
            [
                "No Time Method",
                MockTimeGeneratorNoTimeMethod(current_datetime=1574852918),
                AttributeError,
            ],
            [
                "Wrong return type",
                MockTimeGeneratorWrongReturnType(current_datetime=1574852918),
                ValueError,
            ],
        ]
    )
    def test_create_scope_id_failure(self, _, time_generator, expected_exception):

        with self.assertRaises(expected_exception):
            create_scope_id(time_generator)

    @parameterized.expand(
        [
            [
                "InstrumentDefinition Model",
                lusid.models.InstrumentDefinition,
                ["name", "identifiers"],
            ],
            [
                "Transaction Request Model",
                lusid.models.TransactionRequest,
                [
                    "transaction_id",
                    "type",
                    "instrument_identifiers",
                    "transaction_date",
                    "settlement_date",
                    "units",
                    "transaction_price",
                    "total_consideration",
                ],
            ],
        ]
    )
    def test_get_required_attributes_from_model(
        self, _, model_object, expected_outcome
    ):

        required_attributes = cocoon.utilities.get_required_attributes_from_model(
            model_object
        )

        self.assertEqual(first=expected_outcome, second=required_attributes)

    @parameterized.expand(
        [
            [
                "Not a complex type",
                "InstrumentDefinition",
                "InstrumentDefinition",
                None,
            ],
            [
                "A dict type with a LUSID model",
                "dict(str, ModelProperty)",
                "ModelProperty",
                "dict",
            ],
            ["A dict type with a primitive value", "dict(str, str)", "str", "dict"],
            ["A list type with a LUSID model", "list[TaxLot]", "TaxLot", "list"],
            ["A list type with a primitive value", "list[str]", "str", "list"],
        ]
    )
    def test_extract_lusid_model_from_attribute_type(
        self, _, attribute_type, expected_attribute, expected_nested
    ):

        (
            attribute_type,
            nested_type,
        ) = cocoon.utilities.extract_lusid_model_from_attribute_type(attribute_type)

        self.assertEqual(first=expected_attribute, second=attribute_type)

        self.assertEqual(first=expected_nested, second=nested_type)

    @parameterized.expand(
        [
            (
                "merge FX txns into single line",
                pd.DataFrame(
                    data=[
                        [1000, 1, 1, -500, "GBP", "FW", "by", "2020/01/01"],
                        [1001, 1, 1, 500, "GBP", "FW", "sl", "2020/01/01"],
                        [1002, 1, 1, -500, "GBP", "FW", "sl", "2020/01/01"],
                        [1000, 1, 1, 1000, "EUR", "FW", "sl", "2020/01/01"],
                        [1001, 1, 1, -1000, "USD", "FW", "by", "2020/01/01"],
                        [1002, 1, 1, 1000, "JPY", "FW", "by", "2020/01/01"],
                        [1003, 1, 1, -1000, "JPY", "st", "", "2020/01/01"],
                        [1004, 1, 1, 1000, "JPY", "st", "", "2020/01/01"],
                    ],
                    columns=[
                        "TX_ID",
                        "Price",
                        "price (local)",
                        "quantity",
                        "currency",
                        "type",
                        "leg",
                        "date",
                    ],
                ),
                pd.DataFrame(
                    data=[
                        [
                            1000,
                            1,
                            1,
                            -500,
                            "GBP",
                            "FW",
                            "by",
                            "2020/01/01",
                            1,
                            1,
                            1000,
                            "EUR",
                            "sl",
                            "2020/01/01",
                        ],
                        [
                            1001,
                            1,
                            1,
                            -1000,
                            "USD",
                            "FW",
                            "by",
                            "2020/01/01",
                            1,
                            1,
                            500,
                            "GBP",
                            "sl",
                            "2020/01/01",
                        ],
                        [
                            1002,
                            1,
                            1,
                            1000,
                            "JPY",
                            "FW",
                            "by",
                            "2020/01/01",
                            1,
                            1,
                            -500,
                            "GBP",
                            "sl",
                            "2020/01/01",
                        ],
                    ],
                    columns=[
                        "TX_ID",
                        "Price_b",
                        "price (local)_b",
                        "quantity_b",
                        "currency_b",
                        "type",
                        "leg_b",
                        "date_b",
                        "Price_s",
                        "price (local)_s",
                        "quantity_s",
                        "currency_s",
                        "leg_s",
                        "date_s",
                    ],
                ),
                {
                    "transactions": {
                        "required": {
                            "code": "$fund_id",
                            "settlement_date": "date",
                            "total_consideration.amount": "quantity",
                            "total_consideration.currency": "currency",
                            "transaction_currency": "currency",
                            "transaction_date": "date",
                            "transaction_id": "TX_ID",
                            "transaction_price.price": "Price",
                            "transaction_price.type": "$Price",
                            "type": "type",
                            "units": "quantity",
                        }
                    }
                },
                {
                    "transactions": {
                        "required": {
                            "code": "$fund_id",
                            "settlement_date": "date",
                            "total_consideration.amount": "quantity_b",
                            "total_consideration.currency": "currency_b",
                            "transaction_currency": "currency_s",
                            "transaction_date": "date",
                            "transaction_id": "TX_ID",
                            "transaction_price.price": "Price",
                            "transaction_price.type": "$Price",
                            "type": "type",
                            "units": "quantity_s",
                        }
                    }
                },
                lambda x: x["leg"] == "by",
                lambda x: x["leg"] == "sl",
            )
        ]
    )
    def test_default_fx_forward_model(
        self, _, df, df_gt, mapping, mapping_gt, fun1, fun2
    ):

        df_test, mapping_test = default_fx_forward_model(df, "FW", fun1, fun2, mapping)

        self.assertIsNone(
            assert_frame_equal(df_gt, df_test), msg="Data does not match test case"
        )
        self.assertEqual(mapping_gt, mapping_test, msg="mapping not correctly remapped")

    @parameterized.expand(
        [
            (
                "Replace one single matching value standard syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                ["a2"],
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace two matching values standard standard syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                ["a2", "a3"],
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace all matching search values standard syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                "",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace one single matching value default-column syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "old"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                ["a2"],
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "new"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace two matching values default-column syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "old"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "old"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                ["a2", "a3"],
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "new"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "new"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace all matching search values default-column syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "old"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "old"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                "",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "new",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "new"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {
                            "c1": "old",
                            "c2": {"default": "NotFound", "column": "new"},
                        },
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace one single matching value constant syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "$old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                ["a2"],
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": {"default": "old", "column": "new"},},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace two matching values constant syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "$old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "$old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                ["a2", "a3"],
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": {"default": "old", "column": "new"},},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": {"default": "old", "column": "new"},},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
            (
                "Replace all matching search values constant syntax",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": "$old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": "$old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": "$old",},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
                "c2",
                "new",
                "",
                {
                    "a1": {
                        "b1": {"c1": "old", "c2": {"default": "old", "column": "new"},},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a2": {
                        "b1": {"c1": "old", "c2": {"default": "old", "column": "new"},},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                    "a3": {
                        "b1": {"c1": "old", "c2": {"default": "old", "column": "new"},},
                        "b2": {"c3": "old", "c4": "old",},
                        "b3": {"c5": "old", "c6": "old"},
                    },
                },
            ),
        ]
    )
    def test_update_dict_value(
        self, _, d, search_key, new_value, top_level_values_to_search, gt
    ):

        dict_test = update_dict_value(
            d, search_key, new_value, top_level_values_to_search
        )

        self.assertEqual(gt, dict_test)
