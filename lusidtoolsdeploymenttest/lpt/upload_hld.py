import json
from pandas.io.json import json_normalize
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.either import Either

SHK = "SHK:"
P = "P:"
TOOLNAME = "upload_hld"
TOOLTIP = "Upload holdings in csv or json format"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Upload a Portfolio", ["scope", "portfolio", "input", "date"])
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    taxlot_fields = [
        "units",
        "cost.amount",
        "cost.currency",
        "portfolio_cost",
        "price",
        "purchase_date",
        "settlement_date",
    ]

    # Check if is json or csv
    if "csv" in args.input:
        df = lpt.read_input(args.input)
    elif "json" in args.input:
        # Convert from json to dataframe
        with open(args.input, "r") as myfile:
            data = json.load(myfile)
        df = json_normalize(data, max_level=1)
        df.columns = df.columns.str.replace("subHoldingKeys.", SHK)
        df.columns = df.columns.str.replace("properties.", P)
        columns_to_modify = [
            c for c in df.columns.values if c.startswith(SHK) or c.startswith(P)
        ]
        for col in columns_to_modify:
            df[col] = df[col].apply(lambda x: x.get("value", {}).get("labelValue", ""))
    else:
        raise Exception(
            "The file provided: {} is not .json or .csv format.".format(args.input)
        )

    # Check the schema
    for column in taxlot_fields:
        if column not in df.columns:
            df[column] = None

    keys = ["instrument_uid"] + [c for c in df.columns.values if c.startswith(SHK)]

    # Fill down any blanks
    df[keys] = df[keys].fillna(method="ffill")

    # Group by the keys and build request for each group
    return api.call.set_holdings(
        args.scope,
        args.portfolio,
        lpt.to_date(args.date),
        holding_adjustments=[
            api.models.AdjustHoldingRequest(
                instrument_identifiers=lpt.to_instrument_identifiers(
                    i if len(keys) == 1 else i[0]
                ),
                sub_holding_keys=lpt.perpetual_upsert(api.models, hld_df, SHK),
                properties=lpt.perpetual_upsert(api.models, hld_df),
                tax_lots=api.from_df(
                    hld_df[taxlot_fields], api.models.TargetTaxLotRequest
                ),
            )
            for i, hld_df in df.groupby(keys)
        ],
    )


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
