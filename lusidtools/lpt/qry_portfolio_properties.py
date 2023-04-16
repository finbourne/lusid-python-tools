import pandas as pd
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import lpt

TOOLNAME = "port_props"
TOOLTIP = "Show properties on a portfolio"


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get Portfolios", ["filename", "limit", "portfolio", "scope", "asat"]
        )
        .add("--date")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        def get_value(p):
            return (
                p.key,
                p.value.metric_value.value
                if p.value.metric_value is not None
                else p.value.label_value,
                p.effective_from,
            )

        df = pd.DataFrame.from_records(
            [get_value(p) for p in result.content.properties.values()],
            columns=["key", "value", "effective_from"],
        )

        return lpt.trim_df(df, args.limit, sort="key")

    return api.call.get_portfolio_properties(
        scope=args.scope, code=args.portfolio, effective_at=lpt.to_date(args.date)
    ).bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args)
