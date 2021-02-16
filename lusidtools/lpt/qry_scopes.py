import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.either import Either
from lusidtools.lpt.pager import page_all_results
import datetime

TOOLNAME = "scopes"
TOOLTIP = "List scopes"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Scopes", ["filename", "limit"])
        .add("--portfolios", action="store_true")
        .add("--batch", type=int, default=1000)
        .add("--monitor", action="store_true")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def fetch_page(page_token):
        if args.monitor:
            print("Fetching page:", page_token, str(datetime.datetime.now()))
        return api.call.list_portfolios(page=page_token, limit=args.batch)

    def got_page(result):
        if args.portfolios:
            df = lpt.to_df(
                result,
                ["id.scope", "id.code", "is_derived", "type", "parent_portfolio_id"],
            )
            df.columns = ["Scope", "Portfolio", "Derived", "Type", "Parent"]
        else:
            df = (
                pd.DataFrame({"Scopes": [v.id.scope for v in result.content.values]})
                .groupby("Scopes")
                .size()
                .reset_index()
            )
        return df

    df = page_all_results(fetch_page, got_page)

    if not args.portfolios:
        df = df.groupby("Scopes", as_index=False).sum()

    return lpt.trim_df(
        df, args.limit, sort=["Scope", "Portfolio"] if args.portfolios else "Scopes",
    )


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
