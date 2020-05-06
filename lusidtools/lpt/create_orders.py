import pandas as pd

from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

NAME = "name"
TOOLNAME = "orders_create"
TOOLTIP = "Create Orders"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Upsert Orders", ["filename", "limit", "test"])
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
        df = pd.concat(
            [lpt.read_input(input_file, dtype=str) for input_file in args.input],
            ignore_index=True,
            sort=False,
        )

        if args.mappings:
            df.rename(
                columns=dict(
                    [
                        (s[1], aliases.get(s[0], s[0]))
                        for s in [m.split("=") for m in args.mappings]
                    ]
                ),
                inplace=True,
            )

        prop_keys = [col for col in df.columns.values if col.startswith("P:")]

        identifiers = [col for col in df.columns.values if col in args.identifiers]

        # Identifiers have to be unique
        df = df.drop_duplicates(identifiers)

        def success(r):
            df = lpt.to_df(r.content, ["id"])
            return lpt.trim_df(df, args.limit)

        order_request=api.models.OrderSetRequest(
            order_requests=[
                api.models.OrderRequest(
                    id=api.models.ResourceId(row["id.scope"], row["id.code"]),
                    side=row["side"],
                    quantity=row["quantity"],
                    order_book_id=api.models.ResourceId(row["orderBookId.scope"], row["orderBookId.code"]),
                    portfolio_id=api.models.ResourceId(row["portfolioId.scope"], row["portfolioId.code"]),
                    instrument_identifiers={'Instrument/default/' + identifier: row[identifier] for identifier in identifiers},
                    properties={key[2:]: api.models.ModelProperty(key[2:], api.models.PropertyValue(row[key])) for key in prop_keys if pd.notna(row[key])}
                )
                for idx, row in df.iterrows()
            ]
        )

        if args.test:
            lpt.display_df(df[identifiers + prop_keys + ["id.code"]])
            print(order_request.order_requests)
            exit()

        return api.call.upsert_orders(request=order_request).bind(success)


def main():
    lpt.standard_flow(parse, lse.connect, process_args)
