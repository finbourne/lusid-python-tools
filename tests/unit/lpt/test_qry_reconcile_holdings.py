import unittest

import pandas
from lusid import ResourceListOfReconciliationBreak, ReconciliationBreak, CurrencyAndAmount, PerpetualProperty, \
    PropertyValue, ModelProperty
from pandas._testing import assert_frame_equal

from lusidtools.lpt.qry_reconcile_holdings import parse_reconciled_holdings
from lusidtools.lpt.record import Rec


class QueryReconcileHoldingsTests(unittest.TestCase):
    def test_extracts_breaks(self):
        reconciliation_breaks = ResourceListOfReconciliationBreak(
            values=[
                ReconciliationBreak(
                    instrument_uid="CCY_GBP",
                    instrument_properties=[
                        ModelProperty(
                            key='Instrument/default/Name',
                            value=PropertyValue(label_value="GBP")
                        )
                    ],
                    left_cost=CurrencyAndAmount(0.0, "GBP"),
                    left_units=100.0,
                    right_cost=CurrencyAndAmount(100.0, "GBP"),
                    right_units=100.0,
                    difference_cost=CurrencyAndAmount(100.0, "GBP"),
                    difference_units=100.0,
                    sub_holding_keys={
                        'Transaction/SE-1558/key1': PerpetualProperty(
                            key="Transaction/SE-1558/key1",
                            value=PropertyValue(label_value="<Not Classified>")
                        )
                    }
                ),
                ReconciliationBreak(
                    instrument_uid="CCY_GBP",
                    instrument_properties=[
                        ModelProperty(
                            key='Instrument/default/Name',
                            value=PropertyValue(label_value="GBP")
                        )
                    ],
                    left_cost=CurrencyAndAmount(100.0, "GBP"),
                    left_units=100.0,
                    right_cost=CurrencyAndAmount(0, "GBP"),
                    right_units=100.0,
                    difference_cost=CurrencyAndAmount(-100.0, "GBP"),
                    difference_units=-100.0,
                    sub_holding_keys={
                        'Transaction/SE-1558/key1': PerpetualProperty(
                            key="Transaction/SE-1558/key1",
                            value=PropertyValue(label_value="Foo")
                        )
                    }
                )
            ]
        )

        df = parse_reconciled_holdings(Rec(content=reconciliation_breaks))
        expected_df = pandas.DataFrame({
            "LUID": ["CCY_GBP", "CCY_GBP"],
            "Name": ["GBP", "GBP"],
            "diff_cost": [100.00, -100.00],
            "diff_cost_ccy": ["GBP", "GBP"],
            "left_cost": [0.00, 100.00],
            "left_cost_ccy": ["GBP", "GBP"],
            "left_units": [100.00, 100.00],
            "right_cost": [100.00, 0.00], "right_cost_ccy": ["GBP", "GBP"],
            "right_units": [100.00, 100.00],
            "Transaction/SE-1558/key1": ["<Not Classified>", "Foo"]})
        assert_frame_equal(expected_df, df)
