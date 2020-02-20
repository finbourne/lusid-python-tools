import pandas as pd
import dateutil
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TOOLNAME = "instr_id"
TOOLTIP = "Display configured Instrument Identifiers"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Query Instrument Identifiers", ["filename", "limit"])
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(result):
        df = lpt.to_df(
            result, ["identifier_type", "is_unique_identifier_type", "property_key"]
        )
        df.columns = ["Id", "Unique", "Key"]

        return lpt.trim_df(df, args.limit, sort="Id")

    return api.call.get_instrument_identifier_types().bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
