import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Scopes", ["filename", "limit"])
        .add("--portfolios", action="store_true")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        if args.portfolios:
            df = lpt.to_df(
                result,
                ["id.scope", "id.code", "is_derived", "type", "parent_portfolio_id"],
            )
            df.columns = ["Scope", "Portfolio", "Derived", "Type", "Parent"]
            return lpt.trim_df(df, args.limit, sort=["Scope", "Portfolio"])
        else:
            df = (
                pd.DataFrame({"Scopes": [v.id.scope for v in result.content.values]})
                .groupby("Scopes")
                .size()
                .reset_index()
            )
            return lpt.trim_df(df, args.limit, sort="Scopes")

    return api.call.list_portfolios().bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
