import pandas as pd
import datetime
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

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
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def success(txns):
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
            ("C", "entry_date_time", "EntryDate"),
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

    properties = ["Instrument/default/Name"]
    properties.extend(args.properties or [])

    if args.type != "input" or args.cancels == True:
        # Date range is required for build_transactions endpoint
        if args.start_date == None:
            args.start_date = "1900-01-01"
        if args.end_date == None:
            args.end_date = datetime.datetime.today()

        result = api.call.build_transactions(
            scope=args.scope,
            code=args.portfolio,
            transaction_query_parameters=api.models.TransactionQueryParameters(
                start_date=lpt.to_date(args.start_date),
                end_date=lpt.to_date(args.end_date),
                query_mode="TradeDate",
                show_cancelled_transactions=args.cancels,
            ),
            property_keys=properties,
        )
    else:
        result = api.call.get_transactions(
            args.scope,
            args.portfolio,
            from_transaction_date=lpt.to_date(args.start_date),
            to_transaction_date=lpt.to_date(args.end_date),
            property_keys=properties,
        )

    return result.bind(success)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
