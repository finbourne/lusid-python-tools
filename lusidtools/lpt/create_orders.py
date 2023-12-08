import pandas as pd

from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.lpt import process_input

NAME = "name"
TOOLNAME = "orders_create"
TOOLTIP = "Create Orders"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Create Orders", ["filename", "limit", "test"])
        .add("input", nargs="+")
        .add(
            "--mappings",
            nargs="+",
            help="column name mappings of the form TICKER=col1 etc",
        )
        .add(
            "--identifiers",
            nargs="+",
            default=["ClientInternal", "Figi"],
            help="Identifier types provided",
        )
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    aliases = {
        "CINT": "ClientInternal",
        "FIGI": "Figi",
        "RIC": "P:Instrument/default/RIC",
        "TICKER": "P:Instrument/default/Ticker",
        "ISIN": "P:Instrument/default/Isin",
    }

    if args.input:
        return process_input(aliases, api, args, upsert_orders)


def upsert_orders(api, args, df, identifiers, prop_keys):
    def success(r):
        df = lpt.to_df(r.content.values, ["id"])
        return lpt.trim_df(df, args.limit)

    order_request = api.models.OrderSetRequest(
        order_requests=[
            api.models.OrderRequest(
                id=api.models.ResourceId(scope=row["id.scope"], code=row["id.code"]),
                side=row["side"],
                quantity=int(row["quantity"]),
                order_book_id=api.models.ResourceId(
                    scope=row["orderBookId.scope"], code=row["orderBookId.code"]
                ),
                portfolio_id=api.models.ResourceId(
                    scope=row["portfolioId.scope"], code=row["portfolioId.code"]
                ),
                instrument_identifiers={
                    "Instrument/default/" + identifier: row[identifier]
                    for identifier in identifiers
                },
                properties={
                    key[2:]: api.models.ModelProperty(
                        key=key[2:],
                        value=api.models.PropertyValue(label_value=row[key]),
                    )
                    for key in prop_keys
                    if pd.notna(row[key])
                },
            )
            for idx, row in df.iterrows()
        ]
    )
    if args.test:
        lpt.display_df(df[identifiers + prop_keys + ["id.code"]])
        print(order_request.order_requests)
        exit()

    return api.call.upsert_orders(order_set_request=order_request).bind(success)


def main():
    lpt.standard_flow(parse, lse.connect, process_args)
