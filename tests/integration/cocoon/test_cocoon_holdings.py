import unittest
from pathlib import Path
import pandas as pd
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id


def holding_instrument_identifiers(fake1=False, fake2=False):
    if fake1:
        fake1 = {
            "Instrument/default/Sedol": "FAKESEDOL1",
            "Instrument/default/Isin": "FAKEISIN1",
        }
    else:
        fake1 = None

    if fake2:
        fake2 = {
            "Instrument/default/Sedol": "FAKESEDOL2",
            "Instrument/default/Isin": "FAKEISIN2",
        }
    else:
        fake2 = None

    return [instrument for instrument in [fake1, fake2] if instrument]


def extract_unique_identifiers_from_holdings_response(response):
    unmatched_instruments = [
        holding_adjustment.instrument_identifiers for
        holding_adjustment in
        response["holdings"].get("unmatched_identifiers", [])
    ]

    return list(
        map(
            dict,
            set(
                tuple(sorted(identifiers.items()))
                for identifiers in unmatched_instruments
            ),
        )
    )


class CocoonTestsHoldings(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature(
        "T3-1",
        "T3-2",
        "T3-3",
        "T3-4",
        "T3-5",
        "T3-6",
        "T3-7",
        "T3-8",
        "T3-9",
        "T3-10",
        "T3-11",
        "T3-12",
        "T3-13",
        "T3-14",
        "T3-15",
        "T3-16",
        "T3-17",
        "T3-18",
        "T3-19",
        "T3-20",
        "T3-21",
    )
    @parameterized.expand(
        [
            [
                "Standard successful load",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard load with ~700 portfolios",
                "prime_broker_test",
                "data/holdings-example-large.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with adjustment only",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                True,
                lusid.models.Version,
            ],
            [
                "Standard successful load with string index",
                "prime_broker_test",
                "data/holdings-example-unique-date-string-index.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with duplicate index",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-index.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with unique properties scope",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001_{create_scope_id()}",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in some constants",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": "$3000",
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in a default value for a required field with no specified column",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {"default": "2019-10-08"},
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in a column as a nested dictionary",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {"column": "Effective Date"},
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in a column and default as a nested dictionary",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {
                        "column": "Effective Date",
                        "default": "2019-10-08",
                    },
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Multiple effective dates in a holdings file",
                "prime_broker_test",
                "data/holdings-example.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with sub-holding-keys based on a column that exists in the file",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Security Description"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with sub-holding-keys based on two columns that exists in the file",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Security Description", "Prime Broker"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that is not populated for all rows",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["swap"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that has no pre-existing property definition",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001_{create_scope_id()}",
                ["Prime Broker"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that has no pre-existing property definition and a different scope",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001",
                ["Prime Broker"],
                f"accountview_{create_scope_id()}",
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that has a metric value",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Quantity"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Duplicate column in the source file",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Pass string as tax_lots.units value",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "$1",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Pass integer as tax_lots.units value",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": 1,
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load but with one unmatched identifier. No flag for return_unmatched_identifiers"
                "means that we should not get that unmatched_identifier returned.",
                "prime_broker_test",
                "data/holdings-example-unique-date-one-unmatched-instrument.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
        ]
    )
    def test_load_from_data_frame_holdings_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
        sub_holding_key_scope,
        holdings_adjustment_only,
        expected_outcome,
    ) -> None:
        """
        Test that holdings can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list[str] property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param list sub_holding_keys: The sub holding keys to populate on the adjustments as transaction properties
        :param bool holdings_adjustment_only: Whether to use the adjust_holdings api call rather than set_holdings when
               working with holdings
        :param any expected_outcome: The expected outcome

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="holdings",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            holdings_adjustment_only=holdings_adjustment_only,
            sub_holding_keys_scope=sub_holding_key_scope,
        )

        self.assertGreater(len(responses["holdings"]["success"]), 0)

        self.assertEqual(len(responses["holdings"]["errors"]), 0)

        # Assert that by default no unmatched_identifiers are returned in the response
        self.assertFalse(responses["holdings"].get("unmatched_identifiers", False))

        self.assertTrue(
            expr=all(
                isinstance(success_response.version, expected_outcome)
                for success_response in responses["holdings"]["success"]
            )
        )

    @lusid_feature(
        "T3-22",
        "T3-23",
        "T3-24",
        "T3-25",
        "T3-26",
        "T3-27",
        "T3-28",
        "T3-29",
        "T3-30",
        "T3-31",
        "T3-32",
        "T3-33",
        "T3-34",
        "T3-35",
        "T3-36",
        "T3-37",
        "T3-38",
        "T3-39",
        "T3-40",
        "T3-41",
    )
    @parameterized.expand(
        [
            [
                "Standard successful load with no unmatched instruments - SetHolding",
                "data/holdings-example-unique-date.csv",
                None,
                False,
                0,
                holding_instrument_identifiers(fake1=False, fake2=False),
            ],
            [
                "Standard successful load with one unmatched instrument - SetHolding",
                "data/holdings-example-unique-date-one-unmatched-instrument.csv",
                None,
                False,
                1,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "Standard successful load with one instrument that has a correct Isin and ClientInternal "
                "but fake Sedol so it should still resolve and return no unmatched identifiers - SetHolding",
                "data/holdings-example-unique-date-incorrect-sedol-correct_isin_clientInternal_should_resolve.csv",
                None,
                False,
                0,
                holding_instrument_identifiers(fake1=False, fake2=False),
            ],
            [
                "Standard successful load with one unmatched instrument in two separate adjustments (two portfolios) - SetHolding",
                "data/holdings-example-unique-date-one-unmatched-instrument-duplicated-in-two-adjustments.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "Standard successful load with two unmatched instrument in two separate adjustments (two portfolios) - SetHolding",
                "data/holdings-example-unique-date-two-unmatched-instruments-separate-adjustments.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "Standard successful load with two unmatched instrument in one single adjustment (one portfolio) - SetHolding",
                "data/holdings-example-unique-date-two-unmatched-instruments-single-adjustment.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "Standard successful load with one unmatched instrument in two adjustments across two dates (one portfolio) - SetHolding",
                "data/holdings-example-one-unmatched-instrument-in-two-adjustments-one-portfolio-two-dates.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "Standard successful load with one unmatched instrument in two adjustments one date (two portfolios) - SetHolding",
                "data/holdings-example-same-date-one-unmatched-instrument-duplicated-in-two-adjustments-two-portfolios.csv",
                None,
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "Standard successful load with two unmatched instrument in two adjustments two dates (two portfolios) - SetHolding",
                "data/holdings-example-two-dates-two-unmatched-instruments-separate-adjustments-two-portfolios.csv",
                None,
                False,
                4,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "One portfolio, one sub-holding-key, one date, two instruments - SetHolding",
                "data/holdings-example-one-portfolio-one-shk-one-date-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "One portfolio, one sub-holding-key, two dates, two instruments - SetHolding",
                "data/holdings-example-one-portfolio-one-shk-two-dates-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "One portfolio, two sub-holding-keys, one date, same instrument - SetHolding",
                "data/holdings-example-one-portfolio-two-shks-one-date-one-instrument.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "One portfolio, two sub-holding-keys, one date, two instruments - SetHolding",
                "data/holdings-example-one-portfolio-two-shks-one-date-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "One portfolio, two sub-holding-keys, two dates, two instruments - SetHolding",
                "data/holdings-example-one-portfolio-two-shks-two-dates-two-instruments.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "One portfolio, two sub-holding-keys, two dates, same instrument - SetHolding",
                "data/holdings-example-one-portfolio-two-shks-two-dates-one-instrument.csv",
                ["Security Description"],
                False,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "One portfolio, two sub-holding-keys, two dates, same instrument - AdjustHolding",
                "data/holdings-example-one-portfolio-two-shks-two-dates-one-instrument.csv",
                ["Security Description"],
                True,
                2,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "One portfolio, one sub-holding-key, two dates, two instruments - AdjustHolding",
                "data/holdings-example-one-portfolio-one-shk-two-dates-two-instruments.csv",
                ["Security Description"],
                True,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "Standard successful load with two unmatched instrument in one single adjustment (one portfolio) - AdjustHolding",
                "data/holdings-example-unique-date-two-unmatched-instruments-single-adjustment.csv",
                None,
                True,
                2,
                holding_instrument_identifiers(fake1=True, fake2=True),
            ],
            [
                "Standard successful load with one unmatched instrument - AdjustHolding",
                "data/holdings-example-unique-date-one-unmatched-instrument.csv",
                None,
                True,
                1,
                holding_instrument_identifiers(fake1=True, fake2=False),
            ],
            [
                "Standard successful load with two unmatched instruments across 3 unique identifiers - AdjustHolding",
                "data/holdings-example-unique-date-two-unmatched-instruments-single-adjustment_three_unique_ids.csv",
                None,
                True,
                2,
                [
                    {
                        "Instrument/default/Sedol": "FAKESEDOL1",
                        "Instrument/default/Isin": "FAKEISIN1",
                    },
                    {
                        "Instrument/default/Sedol": "FAKESEDOL1",
                        "Instrument/default/Isin": "FAKEISIN2",
                    },
                ],
            ],
        ]
    )
    def test_load_from_data_frame_holdings_success_with_unmatched_identifiers(
        self,
        _,
        file_name,
        sub_holding_keys,
        holdings_adjustment_only,
        expected_holdings_in_response,
        expected_unmatched_identifiers,
    ) -> None:
        """
        Test that holdings can be loaded successfully

        :param str file_name: The name of the test data file
        :sub_holding_keys
        :param list[instrument_identifiers] expected_unmatched_identifiers: A list of expected instrument_identifiers

        :return: None
        """
        # Unchanged vars that have no need to be passed via param (they would count as duplicate lines)
        scope = "unmatched_holdings_test"
        mapping_required = {
            "code": "FundCode",
            "effective_at": "Effective Date",
            "tax_lots.units": "Quantity",
        }
        mapping_optional = {"tax_lots.cost.currency": "Local Currency Code"}
        identifier_mapping = {
            "Isin": "ISIN Security Identifier",
            "Sedol": "SEDOL Security Identifier",
            "Currency": "is_cash_with_currency",
            "ClientInternal": "Client Internal",
        }
        property_columns = ["Prime Broker"]
        properties_scope = "operations001"
        sub_holding_key_scope = None
        return_unmatched_identifiers = True
        expected_outcome = lusid.models.Version

        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        # Create the portfolios
        portfolio_response = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "FundCode",
                "display_name": "FundCode",
                "base_currency": "Local Currency Code",
            },
            file_type="portfolio",
            mapping_optional={"created": "Created Date"},
        )

        # Assert that all portfolios were created without any issues
        self.assertEqual(len(portfolio_response.get("portfolios").get("errors")), 0)

        # Load in the holdings adjustments
        holding_responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="holdings",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            holdings_adjustment_only=holdings_adjustment_only,
            sub_holding_keys_scope=sub_holding_key_scope,
            return_unmatched_identifiers=return_unmatched_identifiers,
        )

        self.assertGreater(len(holding_responses["holdings"]["success"]), 0)

        self.assertEqual(len(holding_responses["holdings"]["errors"]), 0)

        self.assertEqual(len(holding_responses["holdings"]["unmatched_identifiers"]), expected_holdings_in_response)

        # Assert that the unmatched_identifiers code is hit when the return_unmatched_identifiers is set to True and
        # Check that the lists contain the same elements, in any order (the assert name is misleading)
        self.assertCountEqual(
            extract_unique_identifiers_from_holdings_response(holding_responses),
            expected_unmatched_identifiers,
        )

        self.assertTrue(
            expr=all(
                isinstance(success_response.version, expected_outcome)
                for success_response in holding_responses["holdings"]["success"]
            )
        )

        # Delete the portfolios at the end of the test
        for portfolio in portfolio_response.get("portfolios").get("success"):
            self.api_factory.build(lusid.api.PortfoliosApi).delete_portfolio(
                scope=scope, code=portfolio.id.code
            )
