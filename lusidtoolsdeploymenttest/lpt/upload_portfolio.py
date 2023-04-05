from lusidtools.lpt import lpt, map_instruments as mi
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.either import Either

TOOLNAME = "upload"
TOOLTIP = "Create a portfolio and upload holdings/transactions"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Upload a Portfolio", ["scope", "portfolio"])
        .add("-t", "--transactions", help="csv or excel file of transactions")
        .add(
            "-p",
            "--positions",
            nargs=2,
            metavar=("filename", "YYYY-MM-DD"),
            help="Positions filename and date",
        )
        .add(
            "-c",
            "--create",
            nargs=3,
            metavar=("DISPLAY-NAME", "CURRENCY", "YYYY-MM-DD"),
            help="Provide the display-name, currency and creation date when creating a portfolio",
        )
        .add(
            "-a",
            "--accounting_method",
            choices=["FIFO", "LIFO", "HCF", "LCF"],
            default="AverageCost",
            help="Alternate accounting method. Default is average-cost",
        )
        .add("--shk", nargs="+", help="sub-holding keys")
        .add("--map", action="store_true", help="use mapping tool to set instruments")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def map_method(method):
        translations = {
            "FIFO": "FirstInFirstOut",
            "LIFO": "LastInFirstOut",
            "HCF": "HighestCostFirst",
            "LCF": "LowestCostFirst",
        }
        return translations.get(method, method)

    def create():
        if args.create:
            return api.call.create_portfolio(
                args.scope,
                create_transaction_portfolio_request=api.models.CreateTransactionPortfolioRequest(
                    code=args.portfolio,
                    display_name=args.create[0],
                    base_currency=args.create[1],
                    created=lpt.to_date(args.create[2]),
                    accounting_method=map_method(args.accounting_method),
                    sub_holding_keys=args.shk,
                ),
            )
        else:
            return Either.Right(None)

    def transactions(r=None):
        if args.transactions:
            df = lpt.read_input(args.transactions)
            if args.map:
                mi.map_instruments(api, df, "instrument_uid"),

            def load_transactions(portfolio, txns):
                return api.call.upsert_transactions(
                    args.scope,
                    portfolio,
                    transaction_request=api.from_df(
                        txns, api.models.TransactionRequest
                    ),
                )

            if args.portfolio.lower().startswith("col:"):
                # Multiple portfolios contained in the file. Read the ID from the columns

                portfolio_column = args.portfolio[4:]

                def load_groups(iterator):
                    try:
                        portfolio, df = next(iterator)
                        print("Transactions: {}".format(portfolio))
                        return load_transactions(
                            portfolio, df.drop(portfolio_column, axis=1)
                        ).bind(lambda r: load_groups(iterator))
                    except StopIteration:
                        return Either.Right(None)

                return load_groups(iter(df.groupby(portfolio_column)))
            else:
                # one-off load. The portfolio id is provided
                return load_transactions(args.portfolio, df)

        return Either.Right(None)

    def positions(r=None):
        if args.positions:
            taxlot_fields = [
                "units",
                "cost.amount",
                "cost.currency",
                "portfolio_cost",
                "price",
                "purchase_date",
                "settlement_date",
            ]

            df = lpt.read_input(args.positions[0])

            # Get unique key fields to group by
            keys = ["instrument_uid"] + [
                c for c in df.columns.values if c.startswith("SHK:")
            ]
            # Fill down any blanks
            df[keys] = df[keys].fillna(method="ffill")

            def set_holdings(portfolio, holdings):
                # Group by the keys and build request for each group
                return api.call.set_holdings(
                    args.scope,
                    portfolio,
                    lpt.to_date(args.positions[1]),
                    adjust_holding_request=[
                        api.models.AdjustHoldingRequest(
                            instrument_identifiers=lpt.to_instrument_identifiers(
                                i if len(keys) == 1 else i[0]
                            ),
                            sub_holding_keys=lpt.perpetual_upsert(
                                api.models, hld_df, "SHK:"
                            ),
                            properties=lpt.perpetual_upsert(api.models, hld_df),
                            tax_lots=api.from_df(
                                hld_df[taxlot_fields], api.models.TargetTaxLotRequest
                            ),
                        )
                        for i, hld_df in holdings.groupby(keys)
                    ],
                )

            if args.portfolio.lower().startswith("col:"):
                # Multiple portfolios contained in the file. Read the ID from the columns

                def load_groups(iterator):
                    try:
                        portfolio, df = next(iterator)
                        print("Holdings: {}".format(portfolio))
                        return set_holdings(portfolio, df).bind(
                            lambda r: load_groups(iterator)
                        )
                    except StopIteration:
                        return Either.Right(None)

                return load_groups(iter(df.groupby(args.portfolio[4:])))
            else:
                # one-off load. The portfolio id is provided
                return set_holdings(args.portfolio, df)

        return Either.Right(None)

    return (
        create()
        .bind(positions)
        .bind(transactions)
        .bind(lambda r: Either.Right("Done!"))
    )


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
