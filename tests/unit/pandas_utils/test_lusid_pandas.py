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
        secrets_file = Path(__file__).parent.parent.parent.joinpath('secrets.json')
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.holding_no_props_1 = lusid.models.portfolio_holding.PortfolioHolding(
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

        cls.holding_no_props_2 = lusid.models.portfolio_holding.PortfolioHolding(
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

        cls.holding_1 = lusid.models.portfolio_holding.PortfolioHolding(
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

        cls.test_instrument_response_1 = lusid.models.instrument.Instrument(
            lusid_instrument_id="LUID_TEST1",
            version=1,
            name="Test LUID1",
            identifiers={
                "ClientInternal": "imd_35349922",
                "Figi": "BBG000BRVH22",
                "Isin": "GB00B1KJJ422",
                "LusidInstrumentId": "LUID_ZJJ1IY22",
                "Ticker": "WTB22",
            },
            state="Active",
        )

        cls.test_instrument_response_2 = lusid.models.instrument.Instrument(
            lusid_instrument_id="LUID_TEST2",
            version=1,
            name="Test LUID2",
            identifiers={
                "ClientInternal": "imd_35349900",
                "Figi": "BBG000BRVH05",
                "Isin": "GB00B1KJJ408",
                "LusidInstrumentId": "LUID_ZJJ1IYJS",
                "Ticker": "WTB",
            },
            state="Active",
        )

    def test_response_to_df(self):

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[self.holding_no_props_1, self.holding_no_props_2]
        )

        holdings_df = lusid_response_to_data_frame(test_holdings_response)
        self.assertEqual(type(holdings_df), pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["instrument_uid"], "LUID_XQ6VSO8F")

    def test_instrument_response_to_df(self):

        instrument_df = lusid_response_to_data_frame(self.test_instrument_response_1)
        self.assertEqual(type(instrument_df), pd.DataFrame)
        self.assertEqual(instrument_df.loc["lusid_instrument_id"][0], "LUID_TEST1")

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

        self.assertEqual(type(transaction_alias_df), pd.DataFrame)
        self.assertEqual(transaction_alias_df.loc["transaction_class"][0], "Basic")
        self.assertEqual(type(transaction_alias_df_format), pd.DataFrame)
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

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[self.holding_1]
        )

        holdings_df = lusid_response_to_data_frame(
            test_holdings_response, rename_properties=True
        )

        df_columns = holdings_df.columns

        self.assertEqual(type(holdings_df), pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["instrument_uid"], "LUID_123")
        self.assertIn("PortionClass(MultiAssetScope-SubHoldingKeys)", df_columns)
        self.assertIn("shares_out(MultiAssetScope-Properties)", df_columns)
        self.assertNotIn("JunkBadColumn(SubHoldingKey)", df_columns)

    def test_response_to_df_rename_mapping_format(self):

        rename_mapping = {
            "instrument_uid": "Instrument ID",
            "holding_type": "Holding Type",
            "units": "Units",
            "settled_units": "Settled Units",
            "cost.amount": "Quantity",
            "cost.currency": "Cost Currency",
        }

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[self.holding_1]
        )

        holdings_df = lusid_response_to_data_frame(
            test_holdings_response,
            rename_properties=True,
            column_name_mapping=rename_mapping,
        )

        df_columns = holdings_df.columns

        self.assertEqual(type(holdings_df), pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["Instrument ID"], "LUID_123")
        self.assertIn("PortionClass(MultiAssetScope-SubHoldingKeys)", df_columns)
        self.assertTrue(
            set(
                ["Holding Type", "Units", "Settled Units", "Quantity", "Cost Currency"]
            ).issubset(set(df_columns))
        )

    def test_response_to_df_rename_mapping_no_format(self):

        rename_mapping = {
            "instrument_uid": "Instrument ID",
            "holding_type": "Holding Type",
            "units": "Units",
            "settled_units": "Settled Units",
            "cost.amount": "Quantity",
            "cost.currency": "Cost Currency",
        }

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[self.holding_1]
        )

        holdings_df = lusid_response_to_data_frame(
            test_holdings_response,
            rename_properties=False,
            column_name_mapping=rename_mapping,
        )

        df_columns = holdings_df.columns

        self.assertEqual(type(holdings_df), pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["Instrument ID"], "LUID_123")
        self.assertEqual(
            set(
                ["Holding Type", "Units", "Settled Units", "Quantity", "Cost Currency"]
            ),
            set(
                ["Holding Type", "Units", "Settled Units", "Quantity", "Cost Currency"]
            ).intersection(set(df_columns)),
        )

    def test_response_to_df_rename_mapping_with_column_not_exist(self):

        rename_mapping = {
            "instrument_uid": "Instrument ID",
            "holding_type": "Holding Type",
            "test_not_exist": "Units",
            "settled_units": "Settled Units",
            "cost.amount": "Quantity",
            "cost.currency": "Cost Currency",
        }

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[self.holding_1]
        )

        holdings_df = lusid_response_to_data_frame(
            test_holdings_response,
            rename_properties=False,
            column_name_mapping=rename_mapping,
        )

        df_columns = holdings_df.columns

        self.assertEqual(type(holdings_df), pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["Instrument ID"], "LUID_123")
        self.assertEqual(
            set(["Holding Type", "Quantity", "Cost Currency"]),
            set(["Holding Type", "Quantity", "Cost Currency"]).intersection(
                set(df_columns)
            ),
        )
        self.assertNotIn("settled_units", df_columns)

    def test_response_to_df_rename_property(self):

        rename_mapping = {
            "instrument_uid": "Instrument ID",
            "holding_type": "Holding Type",
            "units": "Units",
            "settled_units": "Settled Units",
            "cost.amount": "Quantity",
            "cost.currency": "Cost Currency",
            "properties.Instrument/MultiAssetScope/shares_out.key": "Shares Outstanding",
        }

        test_holdings_response = lusid.models.versioned_resource_list_of_portfolio_holding.VersionedResourceListOfPortfolioHolding(
            version=1, values=[self.holding_1]
        )

        holdings_df = lusid_response_to_data_frame(
            test_holdings_response,
            rename_properties=False,
            column_name_mapping=rename_mapping,
        )

        df_columns = holdings_df.columns

        self.assertEqual(type(holdings_df), pd.DataFrame)
        self.assertEqual(holdings_df.loc[0]["Instrument ID"], "LUID_123")
        self.assertEqual(
            set(
                [
                    "Holding Type",
                    "Units",
                    "Quantity",
                    "Cost Currency",
                    "Shares Outstanding",
                ]
            ),
            set(
                [
                    "Holding Type",
                    "Units",
                    "Quantity",
                    "Cost Currency",
                    "Shares Outstanding",
                ]
            ).intersection(set(df_columns)),
        )

    def test_list_of_lusid_objects(self):

        list_response = [
            self.test_instrument_response_1,
            self.test_instrument_response_2,
        ]

        instrument_df = lusid_response_to_data_frame(list_response)
        self.assertEqual(type(instrument_df), pd.DataFrame)
        self.assertEqual(
            instrument_df.columns.to_list(),
            [
                "lusid_instrument_id",
                "version",
                "name",
                "identifiers.ClientInternal",
                "identifiers.Figi",
                "identifiers.Isin",
                "identifiers.LusidInstrumentId",
                "identifiers.Ticker",
                "state",
            ],
        )
        self.assertEqual(
            instrument_df["lusid_instrument_id"].to_list()[0], "LUID_TEST1"
        )

    def test_list_of_different_lusid_objects(self):

        with self.assertRaises(TypeError) as error:

            list_response = [
                self.test_instrument_response_1,
                self.test_instrument_response_2,
                self.holding_1,
            ]

            lusid_response_to_data_frame(list_response)

        self.assertEqual(
            error.exception.args[0], "All items in list must be of same data type",
        )

    def test_list_of_same_lusid_objects_no_to_dict(self):

        with self.assertRaises(TypeError) as error:

            bad_list = ["random_string_1", "random_string_2"]

            lusid_response_to_data_frame(bad_list)

        self.assertEqual(
            error.exception.args[0],
            "All object items in list must have a to_dict() method",
        )

    def test_empty_list_lusid_response(self):

        empty_list = []

        empty_df = lusid_response_to_data_frame(empty_list)

        print(empty_df)

        self.assertEqual(empty_df.empty, True)
