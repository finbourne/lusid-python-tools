import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from datetime import datetime

TOOLNAME = "targets"
TOOLTIP = "List and display target holdings"


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get targets",
            ["portfolio", "scope", "date_range", "filename", "limit", "asat"],
        )
        .add("--date", help="YYYY-MM-DD - if provided will query the holdings")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):

    taxlot_fields = [
        "units",
        "cost.currency",
        "cost.amount",
        "portfolio_cost",
        "price",
        "purchase_date",
        "settlement_date",
    ]
    identifiers = set()
    shk = set()
    properties = set()

    def make_holding(adj):
        lots = lpt.to_df(adj.tax_lots, taxlot_fields)
        lots["instrument_uid"] = adj.instrument_uid
        identifiers.update(adj.instrument_identifiers.keys())

        # TODO: sub-holding-keys and properties
        for k, v in adj.instrument_identifiers.items():
            lots[k] = v

        if adj.sub_holding_keys is not None:
            for k, v in adj.sub_holding_keys.items():
                lots[f"SHK:{k}"] = v.value.label_value

        return lots

    def get_success(result):
        holdings = [make_holding(adj) for adj in result.content.adjustments]
        df = pd.concat(holdings, ignore_index=True, sort=True)
        return lpt.trim_df(df, args.limit)

    def list_success(result):
        return lpt.trim_df(
            lpt.to_df(
                result,
                ["effective_at", "unmatched_holding_method", "version.as_at_date"],
            ),
            args.limit,
            sort="effective_at",
        )

    if args.date:
        return api.call.get_holdings_adjustment(
            scope=args.scope, code=args.portfolio, effective_at=lpt.to_date(args.date)
        ).bind(get_success)
    else:
        return api.call.list_holdings_adjustments(
            scope=args.scope,
            code=args.portfolio,
            from_effective_at=lpt.to_date(args.start_date)
            if args.start_date
            else lpt.to_date("1900-01-01"),
            to_effective_at=lpt.to_date(args.end_date)
            if args.end_date
            else lpt.to_date(datetime.now()),
        ).bind(list_success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
