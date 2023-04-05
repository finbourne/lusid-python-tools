import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TOOLNAME = "quotes"
TOOLTIP = "Retrieve quotes"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Quotes", ["scope", "filename", "limit", "asat"])
        .add("date", help="Date for which you require quotes")
        .add("instrument", nargs="*")
        .add(
            "--identifier",
            "-i",
            default="LusidInstrumentId",
            help="Instrument identifer type",
        )
        .add("-p", "--provider", default="", help="Provider")
        .add("-s", "--source", default="", help="Price source")
        .add(
            "-q",
            "--quote-type",
            default="Price",
            choices=["Price", "Rate", "rate"],
            help="quote type",
        )
        .add("--field", dest="field", default="Mid", help="quote field")
        .add(
            "--from",
            dest="from_file",
            nargs=2,
            metavar=("filename.csv", "column"),
            help="load values from file",
        )
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        columns = [
            ("quote_id.effective_at", "Date"),
            ("quote_id.quote_series_id.instrument_id_type", "Instrument Type"),
            ("quote_id.quote_series_id.instrument_id", "Instrument"),
            ("metric_value.value", "Quote"),
            ("metric_value.unit", "Unit"),
            ("quote_id.quote_series_id.quote_type", "Type"),
            ("quote_id.quote_series_id.price_source", "Source"),
            ("quote_id.quote_series_id.provider", "Provider"),
            ("as_at", "AsAt"),
        ]
        df = lpt.to_df(list(result.content.values.values()), [c[0] for c in columns])
        return lpt.trim_df(df.rename(columns=dict(columns)), args.limit)

    if args.from_file:
        column = args.from_file[1]
        df = pd.read_csv(args.from_file[0], dtype=str)[[column]].drop_duplicates()
        df = df[df[column].notnull()]
        args.instrument.extend(df[column].values)

    return api.call.get_quotes(
        scope=args.scope,
        request_body={
            i: api.models.QuoteSeriesId(
                provider=args.provider,
                price_source=args.source,
                instrument_id=i,
                instrument_id_type=args.identifier,
                quote_type=args.quote_type,
                field=args.field,
            )
            for i in args.instrument
        },
        effective_at=lpt.to_date(args.date),
    ).bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args)
