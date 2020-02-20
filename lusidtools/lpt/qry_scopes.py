import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from .either import Either
import re
import urllib.parse

rexp = re.compile(r".*page=([^=']{10,}).*")

TOOLNAME = "scopes"
TOOLTIP = "List scopes"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Scopes", ["filename", "limit"])
        .add("--portfolios", action="store_true")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    results = []

    def fetch_page(page_token):
        return api.call.list_portfolios(page=page_token)

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
        results.append(df)

        links = [l for l in result.content.links if l.relation == "NextPage"]

        if len(links) > 0:
            match = rexp.match(links[0].href)
            if match:
                return urllib.parse.unquote(match.group(1))
        return None

    page = Either(None)
    while True:
        page = fetch_page(page.right).bind(got_page)
        if page.is_left():
            return page
        if page.right == None:
            break

    return lpt.trim_df(
        pd.concat(results, ignore_index=True, sort=False),
        args.limit,
        sort=["Scope", "Portfolio"] if args.portfolios else "Scopes",
    )


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
