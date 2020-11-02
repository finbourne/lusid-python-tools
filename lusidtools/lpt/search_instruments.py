import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TOOLNAME = "instr_search"
TOOLTIP = "Search for Instruments"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Search Instruments", ["filename", "limit"])
        .add("--properties", nargs="*", help="properties to search")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        flat = [i for r in result.content for i in r.mastered_instruments]
        if len(flat) > 0:
            identifiers = sorted(set.union(*[set(i.identifiers.keys()) for i in flat]))
            df = lpt.to_df(
                flat,
                ["name"] + ["identifiers.KEY:" + i + ".value" for i in identifiers],
            )
            df.columns = ["Name"] + identifiers
            return df
        else:
            return "No Matches"

    request = [
        api.models.InstrumentSearchProperty(s[0], s[1])
        for s in [p.split("=") for p in args.properties]
    ]
    return api.call.instruments_search(
        instrument_search_property=request, mastered_only=True
    ).bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    lpt.standard_flow(parse, lse.connect, process_args, display_df)
