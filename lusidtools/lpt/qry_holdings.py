import pandas as pd
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TYPE_COL = "Type"
INSTR_COL = "LUID"
TOOLNAME = "hld"
TOOLTIP = "Query Holdings"

columns = [
    ("instrument_uid", INSTR_COL),
    ("P:Instrument/default/Name", "Instrument"),
    ("holding_type", TYPE_COL),
    ("units", "Units"),
    ("settled_units", "Settled"),
    ("cost.amount", "LocalCost"),
    ("cost.currency", "Currency"),
    ("cost_portfolio_ccy.amount", "ClientCost"),
    ("transaction.type", "Commitment"),
    ("transaction.transaction_date", "TradeDate"),
    ("transaction.settlement_date", "SettleDate"),
    ("transaction.total_consideration.currency", "Settle Currency"),
    ("transaction.total_consideration.amount", "Settle Amount"),
]


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get Holdings", ["filename", "limit", "scope", "portfolio", "asat"]
        )
        .add("dates", nargs="*", metavar="YYYY-MM-DD")
        .add("-i", "--instrument", metavar="instrument-id", help="filter an instrument")
        .add("-m", "--monitor", action="store_true", help="monitor the run")
        .add("-t", "--taxlots", action="store_true", help="view at tax-lot level")
        .add("--properties", nargs="+", default=[])
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):

    if len(args.dates) <= 1:

        # Successful query of a single date
        def success(result):
            (stats, df) = result
            if args.instrument:
                df = df[df[INSTR_COL] == args.instrument]

            return lpt.trim_df(df, args.limit)

        return run_query(
            api, args, args.dates[0] if len(args.dates) == 1 else None
        ).bind(success)

    else:

        i = [1]
        records = []

        # Called on successful query of a single date, add result to records
        def create_record(result):
            (stats, df) = result

            num_pos = len(df)
            rec = df.groupby([TYPE_COL]).size().to_dict()
            rec["#"] = i[0]
            rec["Date"] = d
            rec["Duration"] = stats.duration
            rec["LUSID"] = stats.elapsed
            rec["Id"] = stats.requestId
            rec["Start-Time"] = stats.startTime
            rec["Positions"] = num_pos
            records.append(rec)
            if args.monitor:
                print(
                    "{: 3} {}: Duration: {}, LUSID: {}".format(
                        i[0], d, stats.duration, stats.elapsed
                    )
                )
            i[0] += 1

            return 0 < args.limit < i[0]

        # Called on error/failure from query
        def terminate(error):
            return True  # just terminate the loop

        for d in args.dates:
            if run_query(api, args, d).match(left=terminate, right=create_record):
                break

        if len(records) == 0:
            print("No results")
            return None

        df = pd.DataFrame.from_records(records)

        leading_cols = [
            "#",
            "Start-Time",
            "Date",
            "Duration",
            "LUSID",
            "Id",
            "Positions",
        ]

        cols = set(df.columns.values)
        other_cols = list(sorted(cols - set(leading_cols)))

        for col in other_cols:
            df[col] = df[col].fillna(0).astype(int)

        df = df[leading_cols + other_cols]

    return df


def run_query(api, args, date):
    def success(result):
        shkeys = [
            "SHK:" + shk
            for hld in result.content.values[:1]
            for shk in hld.sub_holding_keys.keys()
        ]

        properties = ["P:" + p for p in args.properties]

        df = lpt.to_df(result, [c[0] for c in columns] + shkeys + properties)
        df.columns = (
            [c[1] for c in columns]
            + [k.split("/")[2] for k in shkeys]
            + [p[8:] for p in args.properties]
        )

        return (result.stats, df)

    return api.call.get_holdings(
        scope=args.scope,
        code=args.portfolio,
        effective_at=lpt.to_date(date),
        by_taxlots=args.taxlots,
        property_keys=["Instrument/default/Name"] + args.properties,
    ).bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
