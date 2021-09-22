import unittest
from lusidtools.extract.group_holdings import get_holdings_for_group
from pathlib import Path
from lusidtools.cocoon.utilities import create_scope_id
from parameterized import parameterized

from lusidtools import logger
import lusid
from lusidtools import cocoon as cocoon
import pandas as pd

from lusid.models import CurrencyAndAmount

# Create the Portfolios, Portfolio Groups and Holdings
scope = create_scope_id()


class CocoonTestsExtractGroupHoldings(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

        cls.scope = scope

        portfolio_holdings = pd.read_csv(
            Path(__file__).parent.joinpath("./data/holdings-example.csv")
        )
        portfolio_groups = pd.read_csv(
            Path(__file__).parent.joinpath("./data/portfolio-groups.csv")
        )

        # Create portfolios
        response = cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=cls.scope,
            data_frame=portfolio_holdings,
            mapping_required={
                "code": "FundCode",
                "display_name": "FundCode",
                "created": "$2010-10-09T08:00:00Z",
                "base_currency": "$AUD",
            },
            mapping_optional={},
            file_type="portfolios",
            property_columns=[],
        )
        assert len(response["portfolios"]["errors"]) == 0

        # Add holdings
        response = cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=cls.scope,
            data_frame=portfolio_holdings,
            mapping_required={
                "code": "FundCode",
                "effective_at": "Effective Date",
                "tax_lots.units": "Quantity",
            },
            mapping_optional={
                "tax_lots.cost.amount": "Local Market Value",
                "tax_lots.cost.currency": "Local Currency Code",
                "tax_lots.portfolio_cost": None,
                "tax_lots.price": None,
                "tax_lots.purchase_date": None,
                "tax_lots.settlement_date": None,
            },
            file_type="holdings",
            identifier_mapping={
                "ClientInternal": "SEDOL Security Identifier",
                "Currency": "is_cash_with_currency",
            },
            property_columns=[],
            holdings_adjustment_only=True,
        )
        assert len(response["holdings"]["errors"]) == 0, len(
            response["holdings"]["errors"]
        )

        # Create groups
        response = cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=cls.scope,
            data_frame=portfolio_groups,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
                "created": "$2010-10-09T08:00:00Z",
            },
            mapping_optional={
                "values.scope": f"${cls.scope}",
                "values.code": "FundCode",
            },
            file_type="portfolio_group",
            property_columns=[],
        )
        assert len(response["portfolio_groups"]["errors"]) == 0

        # Create group with sub-groups
        response = cls.api_factory.build(
            lusid.api.PortfolioGroupsApi
        ).create_portfolio_group(
            scope=cls.scope,
            create_portfolio_group_request=lusid.models.CreatePortfolioGroupRequest(
                code="SubGroups",
                display_name="SubGroups",
                created="2010-10-09T08:00:00Z",
                values=[lusid.models.ResourceId(scope=cls.scope, code="Portfolio-Y")],
                sub_groups=[lusid.models.ResourceId(scope=cls.scope, code="ABC12")],
            ),
        )

        assert isinstance(response, lusid.models.PortfolioGroup)

    def check_holdings_correct(
        self, group_holdings, expected_values, lusid_results_keyed
    ):

        # Check that there are the right number of results and they have the correct keys
        self.assertSetEqual(set(group_holdings.keys()), set(expected_values.keys()))

        if lusid_results_keyed is not None:
            self.assertSetEqual(
                set(group_holdings.keys()), set(lusid_results_keyed.keys())
            )

        # Iterate over each result
        for portfolio, holdings in group_holdings.items():

            # Key the result by the instrument name
            holdings_by_instrument_name = {
                holding.properties["Instrument/default/Name"].value.label_value: holding
                for holding in holdings
            }

            # Check that the units and cost are correct according to manual test data
            for instrument_name, holding in holdings_by_instrument_name.items():
                self.assertEqual(
                    float(holding.cost.amount),
                    float(expected_values[portfolio][instrument_name]["cost"]),
                )
                self.assertEqual(
                    float(holding.units),
                    float(expected_values[portfolio][instrument_name]["units"]),
                )
                self.assertEqual(
                    holding.cost.currency,
                    expected_values[portfolio][instrument_name]["cost.currency"],
                )

                # If this is not grouped by Portfolio, skip checking against LUSID as the merging would have to be
                # done above the API anyway
                if lusid_results_keyed is None:
                    return

                # Check that the units and cost are correct according to LUSID results
                self.assertEqual(
                    float(holding.cost.amount),
                    float(lusid_results_keyed[portfolio][instrument_name].cost.amount),
                )
                self.assertEqual(
                    float(holding.units),
                    float(lusid_results_keyed[portfolio][instrument_name].units),
                )
                self.assertEqual(
                    holding.cost.currency,
                    lusid_results_keyed[portfolio][instrument_name].cost.currency,
                )
                self.assertEqual(
                    holding.cost_portfolio_ccy.currency,
                    lusid_results_keyed[portfolio][
                        instrument_name
                    ].cost_portfolio_ccy.currency,
                )
                self.assertEqual(
                    float(holding.cost_portfolio_ccy.amount),
                    float(
                        lusid_results_keyed[portfolio][
                            instrument_name
                        ].cost_portfolio_ccy.amount
                    ),
                )

    @parameterized.expand(
        [
            [
                "Grouped by portfolio, no sub-groups",
                "ABC12",
                True,
                {
                    f"{scope} : Portfolio-X": {
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                    f"{scope} : Portfolio-Z": {
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                },
            ],
            [
                "Grouped by portfolio, single sub-group",
                "SubGroups",
                True,
                {
                    f"{scope} : Portfolio-X": {
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                    f"{scope} : Portfolio-Z": {
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                        "Tesla Inc": {
                            "cost": "22038434",
                            "cost.currency": "USD",
                            "units": "95421",
                        },
                    },
                    f"{scope} : Portfolio-Y": {
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                    },
                },
            ],
            [
                "Merged together, no sub-groups",
                "ABC12",
                False,
                {
                    f"{scope} : ABC12": {
                        "Amazon": {
                            "cost": "52916721",
                            "cost.currency": "USD",
                            "units": "26598",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "44076868",
                            "cost.currency": "USD",
                            "units": "190842",
                        },
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "2084000",
                            "cost.currency": "USD",
                            "units": "10000",
                        },
                    }
                },
            ],
            [
                "Merged together, single sub-group",
                "SubGroups",
                False,
                {
                    f"{scope} : SubGroups": {
                        "Amazon": {
                            "cost": "105833442",
                            "cost.currency": "USD",
                            "units": "53196",
                        },
                        "Sainsbury": {
                            "cost": "188941977",
                            "cost.currency": "GBP",
                            "units": "942354",
                        },
                        "Tesla Inc": {
                            "cost": "44076868",
                            "cost.currency": "USD",
                            "units": "190842",
                        },
                        "Lloyds Banking Group PLC": {
                            "cost": "141900",
                            "cost.currency": "GBP",
                            "units": "2500",
                        },
                        "Apple Inc": {
                            "cost": "4168000",
                            "cost.currency": "USD",
                            "units": "20000",
                        },
                    }
                },
            ],
        ]
    )
    def test_get_holdings_for_group_grouped_by_portfolio(
        self, _, group_code, group_by_portfolio, expected_results
    ) -> None:

        # Get the group holdings
        group_holdings = get_holdings_for_group(
            api_factory=self.api_factory,
            group_scope=scope,
            group_code=group_code,
            property_keys=["Instrument/default/Name"],
            group_by_portfolio=group_by_portfolio,
            num_threads=15,
        )

        # If not grouping by Portfolio there is no point checking against LUSID as there is no equivalent
        if not group_by_portfolio:
            # Still check against manually prepared data
            return self.check_holdings_correct(group_holdings, expected_results, None)

        # Otherwise get the result from LUSID
        lusid_results = self.api_factory.build(
            lusid.api.PortfolioGroupsApi
        ).get_holdings_for_portfolio_group(
            scope=self.scope, code=group_code, property_keys=["Instrument/default/Name"]
        )

        # Key the LUSID result against portfolio and then instrument name
        lusid_results_keyed = {}
        for holding in lusid_results.values:
            portfolio_scope = holding.properties[
                "Holding/default/SourcePortfolioScope"
            ].value.label_value
            portfolio_code = holding.properties[
                "Holding/default/SourcePortfolioId"
            ].value.label_value
            portfolio = f"{portfolio_scope} : {portfolio_code}"

            instrument_name = holding.properties[
                "Instrument/default/Name"
            ].value.label_value
            lusid_results_keyed.setdefault(portfolio, {}).setdefault(
                instrument_name, holding
            )

        # Check that the result is as expected
        self.check_holdings_correct(
            group_holdings, expected_results, lusid_results_keyed
        )
