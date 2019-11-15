import pandas as pd
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt, map_instruments as mi


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Upload Prices", ["filename", "scope", "date"])
        .add("date", metavar="YYYY-MM-DD")
        .add("--update", action="store_true")
        .add("--fx", action="store_true")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):

    df = lpt.read_csv(args.filename)
    date = lpt.to_date(args.date)
    scope = args.scope

    c1 = df.columns.values[0]  # First column is the instrument

    if args.fx:
        scope = scope + "_FX"
        df[c1] = "CCY_" + df[c1]
        df["column3"] = None
    else:
        mi.map_instruments(api, df, c1)

    # fix the column names
    df.columns = ["instrument", "price", "ccy"]

    def upsert_analytics(result=None):
        analytics = [
            api.models.InstrumentAnalytic(row["instrument"], row["price"], row["ccy"])
            for i, row in df.iterrows()
        ]

        return api.call.set_analytics(
            scope, date.year, date.month, date.day, analytics
        ).bind(lambda r: None)

    if args.update:
        return upsert_analytics()

    return api.call.create_analytic_store(
        api.models.CreateAnalyticStoreRequest(scope, date)
    ).bind(upsert_analytics)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    lpt.standard_flow(parse, lse.connect, process_args)
