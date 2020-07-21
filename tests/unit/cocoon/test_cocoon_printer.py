from pathlib import Path
import lusid
import lusid.models as models
import unittest
from lusidtools import logger
from lusidtools.cocoon.cocoon_printer import (
    format_portfolios_response,
    format_instruments_response,
    format_holdings_response,
    format_quotes_response,
    format_transactions_response,
    get_portfolio_from_href,
    format_reference_portfolios_response,
)
from parameterized import parameterized


# Set up api responses

instrument_success = models.UpsertInstrumentsResponse(
    values={
        "ClientInternal: imd_00001234": models.Instrument(
            lusid_instrument_id="LUID_01234567",
            version=1,
            name="name1",
            identifiers=["Luid", "Figi"],
            state="Active",
        )
    },
    failed={
        "ClientInternal: imd_00001234": models.Instrument(
            lusid_instrument_id="LUID_01234567",
            version=1,
            name="name1",
            identifiers=["Luid", "Figi"],
            state="Active",
        )
    },
)

portfolio_success = models.Portfolio(
    links=[],
    version="1",
    type="Transaction",
    display_name="name2",
    description="test portfolio",
    created="2019-01-01",
    href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    id=models.ResourceId(code="ID00001", scope="default test"),
)

transaction_success = models.UpsertPortfolioTransactionsResponse(
    href="https://www.notadonaim.lusid.com/api/api/code/transactions?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    links=[],
    version="1",
)

quote_success = models.UpsertQuotesResponse(
    failed={
        "BBG001MM1KV4-Figi_2019-10-28": models.Quote(
            as_at="2019-01-16",
            quote_id=models.QuoteId(
                quote_series_id=models.QuoteSeriesId(
                    provider="Default",
                    instrument_id="BBG001MM1KV4",
                    instrument_id_type="Figi",
                    quote_type="Price",
                    field="Mid",
                ),
                effective_at="2019-01-16",
            ),
            uploaded_by="test",
        )
    },
    values={
        "BBG001MM1KV4-Figi_2019-10-28": models.Quote(
            as_at="2019-01-16",
            quote_id=models.QuoteId(
                quote_series_id=models.QuoteSeriesId(
                    provider="Default",
                    instrument_id="BBG001MM1KV4",
                    instrument_id_type="Figi",
                    quote_type="Price",
                    field="Mid",
                ),
                effective_at="2019-01-16",
            ),
            uploaded_by="test",
        )
    },
)

adjust_holding_success = models.AdjustHolding(
    href="https://notadomain.lusid.com/api/api/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    version="1",
)

reference_portfolio_success = models.Portfolio(
    links=[],
    version="1",
    type="Transaction",
    display_name="name3",
    description="test reference portfolio",
    created="2019-01-01",
    href="https://www.tes.lusid/code/portfolio?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
    id=models.ResourceId(code="ID00002", scope="default test"),
)

api_exception = lusid.exceptions.ApiException(status="404", reason="not found")


# build lusidtools responses

responses = {
    "instruments": {
        "errors": [api_exception for _ in range(2)],
        "success": [instrument_success for _ in range(2)],
    },
    "portfolios": {
        "errors": [api_exception for _ in range(2)],
        "success": [portfolio_success for _ in range(2)],
    },
    "transactions": {
        "errors": [api_exception for _ in range(2)],
        "success": [transaction_success for _ in range(2)],
    },
    "quotes": {
        "errors": [api_exception for _ in range(2)],
        "success": [quote_success for _ in range(2)],
    },
    "holdings": {
        "errors": [api_exception for _ in range(2)],
        "success": [adjust_holding_success for _ in range(2)],
    },
    "reference_portfolios": {
        "errors": [api_exception for _ in range(2)],
        "success": [reference_portfolio_success for _ in range(2)],
    },
}

empty_response_with_full_shape = {
    "instruments": {
        "errors": [],
        "success": [models.UpsertInstrumentsResponse(values={}, failed={}),],
    },
    "portfolios": {"errors": [], "success": [],},
    "transactions": {"errors": [], "success": [],},
    "quotes": {
        "errors": [],
        "success": [models.UpsertQuotesResponse(failed={}, values={})],
    },
    "holdings": {"errors": [], "success": [],},
    "reference_portfolios": {"errors": [], "success": [],},
}

empty_response_missing_shape = {
    "instruments": {"errors": [], "success": [],},
    "portfolios": {"errors": [], "success": [],},
    "transactions": {"errors": [], "success": [],},
    "quotes": {"errors": [], "success": []},
    "holdings": {"errors": [], "success": [],},
    "reference_portfolios": {"errors": [], "success": [],},
}

responses_no_error_field = {
    "instruments": {"success": [instrument_success],},
    "portfolios": {"success": [portfolio_success],},
    "transactions": {"success": [transaction_success],},
    "quotes": {"success": [quote_success]},
    "holdings": {"success": [adjust_holding_success],},
    "reference_portfolios": {"success": [portfolio_success],},
}
responses_no_success_field = {
    "instruments": {"errors": [api_exception for _ in range(2)],},
    "portfolios": {"errors": [api_exception for _ in range(2)],},
    "transactions": {"errors": [api_exception for _ in range(2)],},
    "quotes": {"errors": [api_exception for _ in range(2)],},
    "holdings": {"errors": [api_exception for _ in range(2)],},
    "reference_portfolios": {"errors": [api_exception for _ in range(2)],},
}


class CocoonPrinterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")

        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            (
                "standard_quotes",
                [
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/quotes?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                ],
                "quotes",
                ["code", "code1", "code12"],
            ),
            (
                "standard_holdings",
                [
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code1/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                    "asdfasdf/FDaFA/ FFDSSD/234^$^&SEF/code12/holdings?asAt=2019-12-05T10%3A25%3A45.6141270%2B00%3A00",
                ],
                "holdings",
                ["code", "code1", "code12"],
            ),
        ]
    )
    def test_get_portfolio_from_href(self, _, href, file_type, expected_value):
        codes = get_portfolio_from_href(href=href, file_type=file_type)
        self.assertEqual(len(href), len(codes))
        self.assertEqual(expected_value, codes)

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {
                    "succ": [
                        "ClientInternal: imd_00001234",
                        "ClientInternal: imd_00001234",
                    ],
                    "failed": [
                        "ClientInternal: imd_00001234",
                        "ClientInternal: imd_00001234",
                    ],
                    "err": ["not found", "not found"],
                },
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}),
            ("empty_response_missing_shape", empty_response_missing_shape, 0, {}),
        ]
    )
    def test_format_instruments_response_success(
        self, _, response, num_items, expected_value
    ):
        succ, err, failed = format_instruments_response(response)
        self.assertEqual(num_items, len(failed))
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

        [
            self.assertEqual(expected_value["succ"][index], row[succ.columns[0]])
            for index, row in succ.iterrows()
        ]
        [
            self.assertEqual(expected_value["failed"][index], row[failed.columns[0]])
            for index, row in failed.iterrows()
        ]
        [
            self.assertEqual(expected_value["err"][index], row[err.columns[0]])
            for index, row in err.iterrows()
        ]

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["ID00001", "ID00001"], "err": ["not found", "not found"]},
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}),
        ]
    )
    def test_format_portfolios_response_success(
        self, _, response, num_items, expected_value
    ):
        succ, err = format_portfolios_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

        [
            self.assertEqual(expected_value["succ"][index], row[succ.columns[0]])
            for index, row in succ.iterrows()
        ]
        [
            self.assertEqual(expected_value["err"][index], row[err.columns[0]])
            for index, row in err.iterrows()
        ]

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["code", "code"], "err": ["not found", "not found"]},
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}),
        ]
    )
    def test_format_transactions_response_success(
        self, _, response, num_items, expected_value
    ):
        succ, err = format_transactions_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

        [
            self.assertEqual(expected_value["succ"][index], row[succ.columns[0]])
            for index, row in succ.iterrows()
        ]
        [
            self.assertEqual(expected_value["err"][index], row[err.columns[0]])
            for index, row in err.iterrows()
        ]

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {
                    "succ": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "failed": ["BBG001MM1KV4", "BBG001MM1KV4"],
                    "err": ["not found", "not found"],
                },
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}),
            ("empty_response_missing_shape", empty_response_missing_shape, 0, {}),
        ]
    )
    def test_format_quotes_response_success(
        self, _, response, num_items, expected_value
    ):
        succ, err, failed = format_quotes_response(response)
        self.assertEqual(num_items, len(failed))
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

        [
            self.assertEqual(
                expected_value["succ"][index],
                row["quote_id.quote_series_id.instrument_id"],
            )
            for index, row in succ.iterrows()
        ]
        [
            self.assertEqual(
                expected_value["failed"][index],
                row["quote_id.quote_series_id.instrument_id"],
            )
            for index, row in failed.iterrows()
        ]
        [
            self.assertEqual(expected_value["err"][index], row[err.columns[0]])
            for index, row in err.iterrows()
        ]

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["code", "code"], "err": ["not found", "not found"]},
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}),
        ]
    )
    def test_format_holdings_response_success(
        self, _, response, num_items, expected_value
    ):
        succ, err = format_holdings_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

        [
            self.assertEqual(expected_value["succ"][index], row[succ.columns[0]])
            for index, row in succ.iterrows()
        ]
        [
            self.assertEqual(expected_value["err"][index], row[err.columns[0]])
            for index, row in err.iterrows()
        ]

    @parameterized.expand(
        [
            (
                "standard_response",
                responses,
                2,
                {"succ": ["ID00002","ID00002"], "err": ["not found","not found"]},
            ),
            ("empty_response", empty_response_with_full_shape, 0, {}),
        ]
    )
    def test_format_reference_portfolios_response_success(
        self, _, response, num_items, expected_value
    ):
        succ, err = format_reference_portfolios_response(response)
        self.assertEqual(num_items, len(succ))
        self.assertEqual(num_items, len(err))

        [
            self.assertEqual(expected_value["succ"][index], row[succ.columns[0]])
            for index, row in succ.iterrows()
        ]
        [
            self.assertEqual(expected_value["err"][index], row[err.columns[0]])
            for index, row in err.iterrows()
        ]


    # Test failure cases

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_instruments_response_fail(self, _, response, expected_error):
        with self.assertRaises(expected_error):
            succ, err, failed = format_instruments_response(response)

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_portfolios_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_portfolios_response(response)

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_transactions_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_transactions_response(response)

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_quotes_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_quotes_response(response)

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_holdings_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_holdings_response(response)

    @parameterized.expand(
        [
            ("no_error_field", responses_no_error_field, ValueError),
            ("no_success_field", responses_no_success_field, ValueError),
        ]
    )
    def test_format_reference_portfolios_response_fail_no_error_field(
        self, _, response, expected_error
    ):
        with self.assertRaises(expected_error):
            succ, err, failed = format_reference_portfolios_response(response)