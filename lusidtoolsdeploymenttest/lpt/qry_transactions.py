import pandas as pd
import datetime
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from itertools import chain

TOOLNAME = "txn"
TOOLTIP = "Query transactions"


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get Transactions",
            ["filename", "limit", "scope", "portfolio", "date_range", "asat"],
        )
        .add("--properties", nargs="*", help="properties required for display")
        .add(
            "-t",
            "--type",
            metavar="type",
            default="input",
            choices=["input", "output"],
            help="Choose input or output",
        )
        .add("--cancels", action="store_true", help="Show cancelled trades")
        .add("--brief", action="store_true", help="Show fewer data points")
        .add(
            "--pagesize",
            type=int,
            default=5000,
            help="Number of transactions returned per page",
        )
        .extend(extend)
        .parse(args)
    )


def convert_to_dataframe(args, txns):
    available_columns = [
        ("C", "transaction_status", "Status"),
        ("B", "transaction_id", "TxnId"),
        ("B", "type", "Type"),
        ("B", "P:Instrument/default/Name", "Instrument"),
        ("B", "instrument_uid", "LUID"),
        ("B", "transaction_date", "TradeDate"),
        ("A", "settlement_date", "SettleDate"),
        ("B", "units", "Units"),
        ("A", "transaction_price.price", "Price"),
        ("A", "transaction_currency", "TradeCcy"),
        ("A", "total_consideration.currency", "SettleCcy"),
        ("A", "total_consideration.amount", "SettleAmt"),
        ("A", "exchange_rate", "ExchRate"),
        ("A", "P:Transaction/default/TradeToPortfolioRate", "PortRate"),
        ("A", "entry_date_time", "EntryDate"),
        ("C", "cancel_date_time", "CancelDate"),
    ]

    # Pick appropriate set of columns based on arguments

    col_subset = "B" if args.brief else "AB"

    if args.cancels:
        col_subset += "C"

    columns = [c for c in available_columns if c[0] in col_subset]
    columns.extend(
        [
            ("X", "P:" + v, v.replace("Instrument/default/", ""))
            for v in args.properties or []
        ]
    )

    df = lpt.to_df(txns, [c[1] for c in columns])

    # Rename the column headings
    df.columns = [c[2] for c in columns]

    return lpt.trim_df(df, args.limit)


def process_args(api, args):
    properties = ["Instrument/default/Name"]
    properties.extend(args.properties or [])

    txn_fn = None
    txn_fn_args = None

    if args.type != "input" or args.cancels == True:
        # Date range is required for build_transactions endpoint
        if args.start_date == None:
            args.start_date = "1900-01-01"
        if args.end_date == None:
            args.end_date = datetime.datetime.today()

        txn_fn = api.call.build_transactions

        txn_fn_args = dict(
            scope=args.scope,
            code=args.portfolio,
            transaction_query_parameters=api.models.TransactionQueryParameters(
                start_date=lpt.to_date(args.start_date),
                end_date=lpt.to_date(args.end_date),
                query_mode="TradeDate",
                show_cancelled_transactions=args.cancels,
            ),
            property_keys=properties,
            limit=args.pagesize,
        )
    else:
        txn_fn = api.call.get_transactions
        txn_fn_args = dict(
            scope=args.scope,
            code=args.portfolio,
            from_transaction_date=lpt.to_date(args.start_date),
            to_transaction_date=lpt.to_date(args.end_date),
            property_keys=properties,
            limit=args.pagesize,
        )

    all_txns = []
    while True:
        result = txn_fn(**txn_fn_args)
        if result.is_left():
            return result

        content = result.right.content
        all_txns.append(content.values)
        if content.next_page:
            txn_fn_args["page"] = content.next_page
        else:
            return convert_to_dataframe(args, chain.from_iterable(all_txns))


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
