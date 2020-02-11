from pathlib import Path
import lusid
import lusid.models as models
import unittest
from lusidtools.pandas_utils.lusid_pandas import lusid_response_to_data_frame
import pandas as pd
import datetime


class TestResponseToPandasObject(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

    def test_response_to_df(self):

        holding_1 = lusid.models.portfolio_holding.PortfolioHolding(
            cost={"amount": 549997.05, "currency": "GBP"},
            cost_portfolio_ccy={"amount": 0.0, "currency": "GBP"},
            holding_type="P",
            instrument_uid="LUID_XQ6VSO8F",
            properties={},
            settled_units=137088.0,
            sub_holding_keys={},
            transaction=None,
            units=137088.0,
        )

        holding_2 = lusid.models.portfolio_holding.PortfolioHolding(
            cost={"amount": 12345.05, "currency": "GBP"},
            cost_portfolio_ccy={"amount": 0.0, "currency": "GBP"},
            holding_type="P",
            instrument_uid="LUID_123",
            properties={},
            settled_units=1372222.0,
            sub_holding_keys={},
            transaction=None,
            units=1372228.0,
        )

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[holding_1, holding_2]
        )

        holdings_df = lusid_response_to_data_frame(test_holdings_response)
        self.assertIsInstance(holdings_df, pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["instrument_uid"], "LUID_XQ6VSO8F")

    def test_instrument_response_to_df(self):

        test_instrument_response = lusid.models.instrument.Instrument(
            lusid_instrument_id="LUID_TEST",
            version=1,
            name="Test LUID",
            identifiers={},
            state="Active",
        )

        instrument_df = lusid_response_to_data_frame(test_instrument_response)
        self.assertIsInstance(instrument_df, pd.DataFrame)
        self.assertEqual(instrument_df.loc["lusid_instrument_id"][0], "LUID_TEST")

    def test_transaction_alias_response_to_df(self):

        transaction_alias_response = lusid.models.transaction_configuration_type_alias.TransactionConfigurationTypeAlias(
            type="Buy",
            description="Purchase",
            transaction_class="Basic",
            transaction_group="Default",
            transaction_roles="LongLonger",
        )

        transaction_alias_df = lusid_response_to_data_frame(transaction_alias_response)
        transaction_alias_df_format = lusid_response_to_data_frame(
            transaction_alias_response, rename_properties=True
        )

        self.assertIsInstance(transaction_alias_df, pd.DataFrame)
        self.assertEqual(transaction_alias_df.loc["transaction_class"][0], "Basic")
        self.assertIsInstance(transaction_alias_df_format, pd.DataFrame)
        self.assertEqual(
            transaction_alias_df_format.loc["transaction_class"][0], "Basic"
        )

    def test_response_to_df_fail(self):

        self.assertRaises(
            TypeError, lambda: lusid_response_to_data_frame("test_string")
        )

    def test_response_none(self):

        self.assertRaises(TypeError, lambda: lusid_response_to_data_frame(None))

    def test_response_to_df_format(self):

        holding_1 = lusid.models.portfolio_holding.PortfolioHolding(
            cost={"amount": 12345.05, "currency": "GBP"},
            cost_portfolio_ccy={"amount": 0.0, "currency": "GBP"},
            holding_type="P",
            instrument_uid="LUID_123",
            properties={
                "Instrument/MultiAssetScope/shares_out": {
                    "effective_from": datetime.datetime(1, 1, 1, 0, 0),
                    "key": "Instrument/MultiAssetScope/shares_out",
                    "value": {
                        "label_value": None,
                        "metric_value": {"unit": None, "value": 100000.0},
                    },
                },
                "Instrument/MultiAssetScope/sm_instrument_type": {
                    "effective_from": datetime.datetime(1, 1, 1, 0, 0),
                    "key": "Instrument/MultiAssetScope/sm_instrument_type",
                    "value": {"label_value": "Equity", "metric_value": None},
                },
            },
            settled_units=1372222.0,
            sub_holding_keys={
                "Transaction/MultiAssetScope/PortionClass": {
                    "key": "Transaction/MultiAssetScope/PortionClass",
                    "value": {
                        "label_value": "Absolute " "Return " "(other)",
                        "metric_value": None,
                    },
                },
                "Transaction/MultiAssetScope/PortionRegion": {
                    "key": "Transaction/MultiAssetScope/PortionRegion",
                    "value": {"label_value": "None", "metric_value": None},
                },
                "Transaction/MultiAssetScope/PortionSubClass": {
                    "key": "Transaction/MultiAssetScope/PortionSubClass",
                    "value": {
                        "label_value": "Absolute " "Return " "(other)",
                        "metric_value": None,
                    },
                },
            },
            transaction=None,
            units=1372228.0,
        )

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[holding_1]
        )

        holdings_df = lusid_response_to_data_frame(
            test_holdings_response, rename_properties=True
        )

        df_columns = holdings_df.columns

        self.assertIsInstance(holdings_df, pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["instrument_uid"], "LUID_123")
        self.assertIn("PortionClass(MultiAssetScope-SubHoldingKeys)", df_columns)
        self.assertIn("shares_out(MultiAssetScope-Properties)", df_columns)
        self.assertNotIn("JunkBadColumn(SubHoldingKey)", df_columns)
