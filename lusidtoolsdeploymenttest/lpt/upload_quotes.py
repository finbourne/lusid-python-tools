import pandas as pd
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.either import Either


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Upload quotes", ["scope", "filename"])
        .add("input", nargs="+")
        .add("--map", action="store_true", help="use mapping tool to set instruments")
        .extend(extend)
        .parse(args)
    )


def batch_upsert_quotes(api, scope, df):
    quotes = {
        index: api.models.UpsertQuoteRequest(
            quote_id=api.models.QuoteId(
                api.models.QuoteSeriesId(
                    provider=row["provider"],
                    price_source=row["source"],
                    instrument_id=row["instrument_uid"],
                    instrument_id_type=row["instrument_uid_type"],
                    quote_type=row["quote_type"],
                    field=row["field"],
                ),
                effective_at=lpt.to_date(row["effective_at"]),
            ),
            metric_value=api.models.MetricValue(
                value=row["metric_value"], unit=row["metric_unit"]
            ),
            lineage="InternalSystem",
        )
        for index, row in df.iterrows()
    }
    return api.call.upsert_quotes(scope=scope, request_body=quotes)


def process_args(api, args):
    if args.input:
        df = pd.concat(
            [
                lpt.read_input(input_file, dtype=str).fillna("")
                for input_file in args.input
            ],
            ignore_index=True,
            sort=False,
        )

        results = (batch_upsert_quotes(api, args.scope, c) for c in lpt.chunk(df, 2000))

        # Create dict of LUSID upsert quotes failures
        failures = {
            k: v
            for i in results
            if i.is_right
            for k, v in i.right.content.failed.items()
        }
        # Update return df with the errors from LUSID
        updated_failures = update_failures(failures, df)

        # Check no api exceptions for any of the batches
        for f in results:
            if any(f.is_left):
                return f.left

        # If there were any LUSID failures, return the df
        if len(failures) > 0:
            return Either.Right(updated_failures)

    return Either.Right("Success")


def update_failures(res, df):
    df.insert(0, "error", None)
    for key, value in res.items():
        df.at[int(key), "error"] = value.detail.replace(
            "One or more problems occurred. Failures:", ""
        )
    return df.dropna(subset=["error"]).loc[
        :,
        [
            "error",
            "provider",
            "source",
            "instrument_uid",
            "instrument_uid_type",
            "quote_type",
            "field",
        ],
    ]


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
