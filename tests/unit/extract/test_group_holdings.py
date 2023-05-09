import logging
import os
import unittest
from lusidtools import logger
from datetime import datetime
from parameterized import parameterized
import pytz

from lusidtools.extract.group_holdings import _join_holdings
from lusid.models import (
    PortfolioHolding,
    ModelProperty,
    PropertyValue,
    CurrencyAndAmount,
)

now = datetime.now(pytz.UTC)


class CocoonExtractGroupHoldingsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    class PortfolioHoldingTemplate(PortfolioHolding):
        """
        This class provides the ability for overriding the template as you see fit
        """

        def __init__(self, **override_init_kwargs):
            default_init_kwargs = {
                "instrument_uid": "LUID_12345678",
                "properties": {
                    "MyProperty": ModelProperty(
                        key="MyProperty", value=PropertyValue(label_value="blah")
                    )
                },
                "holding_type": "B",
                "units": 100,
                "settled_units": 100,
                "cost": CurrencyAndAmount(amount=10000, currency="AUD"),
                "cost_portfolio_ccy": CurrencyAndAmount(amount=0, currency="AUD"),
            }

            init_kwargs = {**default_init_kwargs, **override_init_kwargs}

            super().__init__(**init_kwargs)

    @parameterized.expand(
        [
            [
                "Standard joining preserving portfolio separation",
                {
                    "MyScope : PortfolioA": [PortfolioHoldingTemplate()],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                True,
                None,
                {
                    "MyScope : PortfolioA": [PortfolioHoldingTemplate()],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
            ],
            [
                "Standard joining with no portfolio separation",
                {
                    "MyScope : PortfolioA": [PortfolioHoldingTemplate()],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "units": 200,
                                "settled_units": 200,
                                "cost": CurrencyAndAmount(amount=20000, currency="AUD"),
                                "properties": {},
                            }
                        )
                    ]
                },
            ],
            [
                "Standard joining with no portfolio separation with Instrument property",
                {
                    "MyScope : PortfolioA": [
                        PortfolioHoldingTemplate(
                            **{
                                "properties": {
                                    "Instrument/default/Name": ModelProperty(
                                        key="Instrument/default/Name",
                                        value=PropertyValue(label_value="Apple"),
                                    )
                                }
                            }
                        )
                    ],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "units": 200,
                                "settled_units": 200,
                                "cost": CurrencyAndAmount(amount=20000, currency="AUD"),
                                "properties": {
                                    "Instrument/default/Name": ModelProperty(
                                        key="Instrument/default/Name",
                                        value=PropertyValue(label_value="Apple"),
                                    )
                                },
                            }
                        )
                    ]
                },
            ],
            [
                "All merged with same instrument in different currencies",
                {
                    "MyScope : PortfolioA": [
                        PortfolioHoldingTemplate(
                            **{"cost": CurrencyAndAmount(amount=1500, currency="USD")}
                        )
                    ],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "cost": CurrencyAndAmount(amount=1500, currency="USD"),
                                "properties": {},
                            }
                        ),
                        PortfolioHoldingTemplate(**{"properties": {}}),
                    ]
                },
            ],
            [
                "2x Instrument A, 1x Instrument B",
                {
                    "MyScope : PortfolioA": [
                        PortfolioHoldingTemplate(
                            **{
                                "cost": CurrencyAndAmount(amount=1500, currency="USD"),
                                "instrument_uid": "LUID_ABD29433",
                            }
                        ),
                        PortfolioHoldingTemplate(),
                    ],
                    "MyScope : PortfolioB": [PortfolioHoldingTemplate()],
                },
                False,
                "PortGroupScope : MyPortGroup",
                {
                    "PortGroupScope : MyPortGroup": [
                        PortfolioHoldingTemplate(
                            **{
                                "cost": CurrencyAndAmount(amount=1500, currency="USD"),
                                "instrument_uid": "LUID_ABD29433",
                                "properties": {},
                            }
                        ),
                        PortfolioHoldingTemplate(
                            **{
                                "units": 200,
                                "settled_units": 200,
                                "cost": CurrencyAndAmount(amount=20000, currency="AUD"),
                                "properties": {},
                            }
                        ),
                    ]
                },
            ],
        ]
    )
    def test_join_holdings(
        self, _, holdings_to_join, group_by_portfolio, dict_key, expected_outcome
    ):
        joined_holdings = _join_holdings(
            holdings_to_join=holdings_to_join,
            group_by_portfolio=group_by_portfolio,
            dict_key=dict_key,
        )

        logging.info(joined_holdings)
        logging.info(expected_outcome)

        self.assertEqual(joined_holdings, expected_outcome)
