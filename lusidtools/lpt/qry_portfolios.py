import pandas as pd
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt

TOOLNAME = "portfolios"
TOOLTIP = "List portfolios for a scope"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Portfolios", ["filename", "limit", "scope", "properties"])
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        colmap = {"P:" + p: p[10:] for p in args.properties}
        df = lpt.to_df(
            result.content,
            ["id.code", "display_name", "created", "parent_portfolio_id"]
            + list(colmap.keys()),
        ).rename(columns=colmap)

        return lpt.trim_df(df, args.limit, sort="id.code")

    return api.call.list_portfolios_for_scope(
        args.scope, property_keys=args.properties
    ).bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
