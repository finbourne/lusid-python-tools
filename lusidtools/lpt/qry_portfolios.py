import pandas as pd
import re
import urllib.parse

from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt
from lusidtools.lpt.either import Either

TOOLNAME = "portfolios"
TOOLTIP = "List portfolios for a scope"

rexp = re.compile(r".*page=([^=']{10,}).*")


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get Portfolios", ["filename", "limit", "scope", "properties", "asat"]
        )
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    results = []

    colmap = {"P:" + p: p[10:] for p in args.properties}

    def fetch_page(page_token):
        return api.call.list_portfolios_for_scope(
            args.scope, property_keys=args.properties, limit=2000, page=page_token
        )

    def got_page(result):
        df = lpt.to_df(
            result.content,
            [
                "id.code",
                "display_name",
                "base_currency",
                "description",
                "created",
                "parent_portfolio_id",
            ]
            + list(colmap.keys()),
        ).rename(columns=colmap)
        results.append(df)

        links = [l for l in result.content.links if l.relation == "NextPage"]

        if len(links) > 0:
            match = rexp.match(links[0].href)
            if match:
                return urllib.parse.unquote(match.group(1))
        return None

        return lpt.trim_df(df, args.limit, sort="id.code")

    page = Either(None)
    while True:
        page = fetch_page(page.right).bind(got_page)
        if page.is_left():
            return page
        if page.right == None:
            break

    return lpt.trim_df(
        pd.concat(results, ignore_index=True, sort=False), args.limit, sort="id.code"
    )


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
