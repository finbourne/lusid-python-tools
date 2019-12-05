from pathlib import Path
import lusid
import lusid.models as models
import unittest
from lusidtools import logger
from lusidtools.cocoon.cocoon_printer import *
from parameterized import parameterized

responses = {
    "instruments": {
        "errors": [
            lusid.exceptions.ApiException(status="404", reason="not found"),
            lusid.exceptions.ApiException(status="404", reason="not found")
        ],
        "success": [
            models.UpsertInstrumentsResponse(values={
                "ClientInternal: imd_00001234":
                    models.Instrument(lusid_instrument_id="LUID_01234567",
                                      version=1,
                                      name="name1",
                                      identifiers=["Luid", "Figi"],
                                      state="Active")
            }, failed={
                "ClientInternal: imd_00001234":
                    models.Instrument(lusid_instrument_id="LUID_01234567",
                                      version=1,
                                      name="name1",
                                      identifiers=["Luid", "Figi"],
                                      state="Active")
            }),
            models.UpsertInstrumentsResponse(values={
                "ClientInternal: imd_00001234":
                    models.Instrument(lusid_instrument_id="LUID_01234567",
                                      version=1,
                                      name="name1",
                                      identifiers=["Luid", "Figi"],
                                      state="Active")
            }, failed={
                "ClientInternal: imd_00001234":
                    models.Instrument(lusid_instrument_id="LUID_01234567",
                                      version=1,
                                      name="name1",
                                      identifiers=["Luid", "Figi"],
                                      state="Active")
            })
        ],
    },
    "portfolios": {
        "errors": [
            lusid.exceptions.ApiException(status="404", reason="not found"),
            lusid.exceptions.ApiException(status="404", reason="not found")
        ],
        "success": [
            models.Portfolio(
                links=[],
                version="1",
                type="Transaction",
                display_name="name2",
                description="test portfolio",
                created="2019-01-01",
                href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                id=models.ResourceId(
                    code="ID00001",
                    scope="default test"
                )
            ),
            models.Portfolio(
                links=[],
                version="1",
                type="Transaction",
                display_name="name2",
                description="test portfolio",
                created="2019-01-01",
                href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                id=models.ResourceId(
                    code="ID00001",
                    scope="default test"
                )
            )
        ],
    },
    "transactions": {
        "errors":
            [
                lusid.exceptions.ApiException(status="404", reason="not found"),
                lusid.exceptions.ApiException(status="404", reason="not found")
            ],
        "success":
            [
                models.UpsertPortfolioTransactionsResponse(
                    href="https://www.notadonaim.lusid.com/api/api/code/transactions?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    links=[],
                    version="1"
                ),
                models.UpsertPortfolioTransactionsResponse(
                    href="https://www.notadonaim.lusid.com/api/api/code/transactions?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    links=[],
                    version="1"
                )
            ],
    },
    "quotes": {
        "errors": [
                lusid.exceptions.ApiException(status="404", reason="not found"),
                lusid.exceptions.ApiException(status="404", reason="not found")
            ],
        "success": [
                models.UpsertQuotesResponse(
                    failed={
                        "BBG001MM1KV4-Figi_2019-10-28":
                            models.Quote(
                                as_at="2019-01-16",
                                quote_id=models.QuoteId(
                                    quote_series_id=models.QuoteSeriesId(
                                        provider="Default",
                                        instrument_id="BBG001MM1KV4",
                                        instrument_id_type="Figi",
                                        quote_type="Price",
                                        field="Mid",
                                    ),
                                    effective_at="2019-01-16"
                                ),
                                uploaded_by="test"
                            )
                    },
                    values={
                        "BBG001MM1KV4-Figi_2019-10-28":
                            models.Quote(
                                as_at="2019-01-16",
                                quote_id=models.QuoteId(
                                    quote_series_id=models.QuoteSeriesId(
                                        provider="Default",
                                        instrument_id="BBG001MM1KV4",
                                        instrument_id_type="Figi",
                                        quote_type="Price",
                                        field="Mid",
                                    ),
                                    effective_at="2019-01-16"
                                ),
                                uploaded_by="test"
                            )
                    }
                ),
                models.UpsertQuotesResponse(
                    failed={
                        "BBG001MM1KV4-Figi_2019-10-28":
                            models.Quote(
                                as_at="2019-01-16",
                                quote_id=models.QuoteId(
                                    quote_series_id=models.QuoteSeriesId(
                                        provider="Default",
                                        instrument_id="BBG001MM1KV4",
                                        instrument_id_type="Figi",
                                        quote_type="Price",
                                        field="Mid",
                                    ),
                                    effective_at="2019-01-16"
                                ),
                                uploaded_by="test"
                            )
                    },
                    values={
                        "BBG001MM1KV4-Figi_2019-10-28":
                            models.Quote(
                                as_at="2019-01-16",
                                quote_id=models.QuoteId(
                                    quote_series_id=models.QuoteSeriesId(
                                        provider="Default",
                                        instrument_id="BBG001MM1KV4",
                                        instrument_id_type="Figi",
                                        quote_type="Price",
                                        field="Mid",
                                    ),
                                    effective_at="2019-01-16"
                                ),
                                uploaded_by="test"
                            )
                    }
                )
            ]
    },
    "holdings": {
        "errors": [
            lusid.exceptions.ApiException(status="404", reason="not found"),
            lusid.exceptions.ApiException(status="404", reason="not found")
        ],
        "success": [
            models.AdjustHolding(
                href="https://notadomain.lusid.com/api/api/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                version="1",
            ),
            models.AdjustHolding(
                href="https://notadomain.lusid.com/api/api/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                version="1",
            )
        ],
    },
}
empty_response_with_full_shape = {
    "instruments": {
        "errors": [],
        "success": [
            models.UpsertInstrumentsResponse(values={}, failed={}),
        ],
    },
    "portfolios": {
        "errors": [],
        "success": [],
    },
    "transactions": {
        "errors": [],
        "success":
            [],
    },
    "quotes": {
        "errors":
            [],
        "success":
            [
                models.UpsertQuotesResponse(
                    failed={},
                    values={}
                )
            ]
    },
    "holdings": {
        "errors": [],
        "success": [],
    },
}
empty_response_missing_shape = {
    "instruments": {
        "errors": [],
        "success": [],
    },
    "portfolios": {
        "errors": [],
        "success": [],
    },
    "transactions": {
        "errors": [],
        "success":
            [],
    },
    "quotes": {
        "errors":
            [],
        "success":
            []
    },
    "holdings": {
        "errors": [],
        "success": [],
    },
}
responses_no_error_field = {
    "instruments": {
        "success": [
            models.UpsertInstrumentsResponse(
                values=
                {
                    "ClientInternal: imd_00001234":
                        models.Instrument(lusid_instrument_id="LUID_01234567",
                                          version=1,
                                          name="name1",
                                          identifiers=["Luid", "Figi"],
                                          state="Active")
                },
                failed=
                {
                    "ClientInternal: imd_00001234":
                        models.Instrument(lusid_instrument_id="LUID_01234567",
                                          version=1,
                                          name="name1",
                                          identifiers=["Luid", "Figi"],
                                          state="Active")
                }
            )
        ],
    },
    "portfolios": {
        "success": [
            models.Portfolio(
                links=[],
                version="1",
                type="Transaction",
                display_name="name2",
                description="test portfolio",
                created="2019-01-01",
                href="https://www.tes.lusid/code/portfolios?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                id=models.ResourceId(
                    code="ID00001",
                    scope="default test"
                )
            )
        ],
    },
    "transactions": {
        "success":
            [
                models.UpsertPortfolioTransactionsResponse(
                    href="https://www.notadonaim.lusid.com/api/api/code/portfolios?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    links=[],
                    version="1"
                )
            ],
    },
    "quotes": {
        "success":
            [
                models.UpsertQuotesResponse(
                    failed={
                        "BBG001MM1KV4-Figi_2019-10-28":
                            models.Quote(
                                as_at="2019-01-16",
                                quote_id=models.QuoteId(
                                    quote_series_id=models.QuoteSeriesId(
                                        provider="Default",
                                        instrument_id="BBG001MM1KV4",
                                        instrument_id_type="Figi",
                                        quote_type="Price",
                                        field="Mid",
                                    ),
                                    effective_at="2019-01-16"
                                ),
                                uploaded_by="test"
                            )
                    },
                    values={
                        "BBG001MM1KV4-Figi_2019-10-28":
                            models.Quote(
                                as_at="2019-01-16",
                                quote_id=models.QuoteId(
                                    quote_series_id=models.QuoteSeriesId(
                                        provider="Default",
                                        instrument_id="BBG001MM1KV4",
                                        instrument_id_type="Figi",
                                        quote_type="Price",
                                        field="Mid",
                                    ),
                                    effective_at="2019-01-16"
                                ),
                                uploaded_by="test"
                            )
                    }

                )
            ]
    },
    "holdings": {
        "success": [
            models.AdjustHolding(
                href="https://notadomain.lusid.com/api/api/code/portfolios?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                version="1",
            )
        ],
    },
}
responses_no_success_field = {
    "instruments": {
        "errors": [
            lusid.exceptions.ApiException(status="404", reason="not found"),
            lusid.exceptions.ApiException(status="404", reason="not found")
        ]
    },
    "portfolios": {
        "errors": [
            lusid.exceptions.ApiException(status="404", reason="not found"),
            lusid.exceptions.ApiException(status="404", reason="not found")
        ]
    },
    "transactions": {
        "errors":
            [
                lusid.exceptions.ApiException(status="404", reason="not found"),
                lusid.exceptions.ApiException(status="404", reason="not found")
            ]
    },
    "quotes": {
        "errors":
            [
                lusid.exceptions.ApiException(status="404", reason="not found"),
                lusid.exceptions.ApiException(status="404", reason="not found")
            ]
    },
    "holdings": {
        "errors": [
            lusid.exceptions.ApiException(status="404", reason="not found"),
            lusid.exceptions.ApiException(status="404", reason="not found")
        ]
    },
}


class CocoonPrinterTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")

        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand([
        ("standard_quotes",
         ["asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
          "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
          "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00"],
         "quotes",
         ["code", "code1", "code12"]),
        ("standard_holdings",
         ["asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
          "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
          "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00"],
         "holdings",
         ["code", "code1", "code12"])
    ])
    def test_get_portfolio_from_href(self, _, href, file_type, ground_truth):
        codes = get_portfolio_from_href(href=href, file_type=file_type)
        self.assertEqual(len(href), len(codes))
        self.assertEqual(ground_truth, codes)


    @parameterized.expand([
        ("standard_response", responses, 2),
        ("empty_response", empty_response_with_full_shape, 0),
        ("empty_response_missing_shape", empty_response_missing_shape, 0)
    ])
    def test_format_instruments_response_success(self, _, response, num_items):
        succ, err, failed = format_instruments_response(response)
        self.assertEqual(num_items, len(failed))
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

    @parameterized.expand([
        ("standard_response", responses, 2),
        ("empty_response", empty_response_with_full_shape, 0)
    ])
    def test_format_portfolios_response_success(self, _, response, num_items):
        succ, err = format_portfolios_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

    @parameterized.expand([
        ("standard_response", responses, 2),
        ("empty_response", empty_response_with_full_shape, 0)
    ])
    def test_format_transactions_response_success(self, _, response, num_items):
        succ, err = format_transactions_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

    @parameterized.expand([
        ("standard_response", responses, 2),
        ("empty_response", empty_response_with_full_shape, 0),
        ("empty_response_missing_shape", empty_response_missing_shape, 0)
    ])
    def test_format_quotes_response_success(self, _, response, num_items):
        succ, err, failed = format_quotes_response(response)
        self.assertEqual(num_items, len(failed))
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

    @parameterized.expand([
        ("standard_response", responses, 2),
        ("empty_response", empty_response_with_full_shape, 0)
    ])
    def test_format_holdings_response_success(self, _, response, num_items):
        succ, err = format_holdings_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

    # Test failure cases

    @parameterized.expand([
        ("no_error_field", responses_no_error_field, ValueError),
        ("no_success_field", responses_no_success_field, ValueError)
    ])
    def test_format_instruments_response_fail(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_instruments_response(response)

    @parameterized.expand([
        ("no_error_field", responses_no_error_field, ValueError),
        ("no_success_field", responses_no_success_field, ValueError)
    ])
    def test_format_portfolios_response_fail_no_error_field(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_portfolios_response(response)

    @parameterized.expand([
        ("no_error_field", responses_no_error_field, ValueError),
        ("no_success_field", responses_no_success_field, ValueError)
    ])
    def test_format_transactions_response_fail_no_error_field(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_transactions_response(response)

    @parameterized.expand([
        ("no_error_field", responses_no_error_field, ValueError),
        ("no_success_field", responses_no_success_field, ValueError)
    ])
    def test_format_quotes_response_fail_no_error_field(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_quotes_response(response)

    @parameterized.expand([
        ("no_error_field", responses_no_error_field, ValueError),
        ("no_success_field", responses_no_success_field, ValueError)
    ])
    def test_format_holdings_response_fail_no_error_field(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_holdings_response(response)
