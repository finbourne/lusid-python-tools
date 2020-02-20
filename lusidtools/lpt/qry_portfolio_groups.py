import pandas as pd
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt

TOOLNAME = "port_grp"
TOOLTIP = "Query Portfolio Groups"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Portfolio Groups", ["filename", "limit", "scope"])
        .add("--group")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success_list_groups(result):
        df = lpt.to_df(result.content, ["id.code", "display_name", "description"])
        return lpt.trim_df(df, args.limit, sort="id.code")

    def success_get_group(result):
        df = lpt.to_df(result.content.portfolios, ["scope", "code",])
        return lpt.trim_df(df, args.limit, sort=["scope", "code"])

    if args.group:
        return api.call.get_portfolio_group(args.scope, args.group).bind(
            success_get_group
        )
    else:
        return api.call.list_portfolio_groups(args.scope).bind(success_list_groups)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
