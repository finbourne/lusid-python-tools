import pandas as pd
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt

# Jira: SENG-40 - Property search was deprecated and this query requires updating
# Uncomment tooltips when fixed
# TOOLNAME = "props"
# TOOLTIP = "Query property definitions"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Property definitions", ["filename", "limit"])
        .add(
            "-s",
            "--scope",
            metavar="scope",
            help="Scope. Use default for system properties",
        )
        .add("-d", "--domain", metavar="domain", help="Property domain")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        return lpt.trim_df(
            lpt.to_df(result, ["scope", "domain", "code", "type", "display_name"],),
            args.limit,
            sort=["scope", "domain", "code"],
        )

    limit = args.limit if args.limit > 0 else 1000
    elasticQuery = {"size": limit, "from": 0}
    constraints = []

    if args.scope:
        constraints.append({"match": {"scope": args.scope}})

    if args.domain:
        constraints.append({"match": {"domain": args.domain}})

    if len(constraints) > 0:
        elasticQuery["query"] = {"bool": {"must": constraints}}

    return api.call.properties_search(body=elasticQuery).bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    raise NotImplementedError(
        "Jira: SENG-40 - Property search was deprecated and this query requires updating"
    )
    # return lpt.standard_flow(parse, lse.connect, process_args, display_df)
