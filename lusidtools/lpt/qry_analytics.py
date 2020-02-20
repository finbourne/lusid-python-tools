import pandas as pd
import numpy as np
import dateutil
import sys
import argparse
import lusidtools.lpt.lpt as lpt
from lusidtools.lpt import lse
import lse

TOOLNAME = "analytics"
TOOLTIP = "Query the analytics"


def parse(extend=None):
    parser = argparse.ArgumentParser(description="List Analytics")
    parser.add_argument(
        "-f", "--filename", metavar="filename.csv", help="write to this file"
    )
    parser.add_argument(
        "--secrets-file",
        dest="secrets",
        default="secrets.json",
        help="path to secrets file",
    )
    if extend:
        extend(parser)
    return parser.parse_args()


def process_args(args):
    api = lse.api(args.secrets)
    df = lse.arrayToDf(
        api.client.list_analytic_stores().values, ["scope", "date_property"]
    )

    df["type"] = "Prices"
    df.loc[df["scope"].str.endswith("_FX"), "type"] = "FX"

    df["date"] = pd.to_datetime(df["date_property"], utc=True).dt.strftime("%Y-%m-%d")
    df = (
        df[["date", "type", "scope"]]
        .sort_values(["date", "scope", "type"])
        .reset_index(drop=True)
    )

    if args.filename:
        df.to_csv(args.filename, index=False)
    else:
        pd.set_option("display.width", None)
        pd.options.display.float_format = "{:,.2f}".format
        pd.set_option("display.max_rows", 1000)
        print(df.fillna(""))


def main(parse=parse):
    process_args(parse)


if __name__ == "__main__":
    import lse

    main()
else:
    from . import lse
