import pandas as pd

from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt
from lusidtools.lpt.either import Either
from lusidtools.lpt.pager import page_all_results

TOOLNAME = "portfolios"
TOOLTIP = "List portfolios for a scope"

ID_CODE = "id.code"


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get Portfolios", ["filename", "limit", "scope", "properties", "asat"]
        )
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):

    colmap = {"P:" + p: p[10:] for p in args.properties}

    def fetch_page(page_token):
        return api.call.list_portfolios_for_scope(
            args.scope, property_keys=args.properties, limit=2000, page=page_token
        )

    def got_page(result):
        return lpt.to_df(
            result.content,
            [
                ID_CODE,
                "display_name",
                "base_currency",
                "description",
                "created",
                "parent_portfolio_id",
            ]
            + list(colmap.keys()),
        ).rename(columns=colmap)

    return lpt.trim_df(page_all_results(fetch_page, got_page), args.limit, sort=ID_CODE)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
