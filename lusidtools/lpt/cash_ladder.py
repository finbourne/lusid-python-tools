import pandas as pd
import numpy as np
import dateutil
import sys
import argparse

SDATE = "settlement_date"
CCY = "security_uid"
QTY = "units"
TYPE = "holding_type"
CUM = "cum"
ORDER = "sort"
JOIN = "join"

TOOLTIP = "Demo Cash-Ladder report"


def cash_ladder(api, scope, portfolio, date):

    qry_date = pd.to_datetime(date, utc=True)

    # Function to make sure there are cash positions
    def check_contents(df):
        if len(df) == 0:
            print(
                "Portfolio {} in scope {} contains no cash on {:%Y-%m-%d}".format(
                    portfolio, scope, start_date
                )
            )
            exit()

    # Run one-day earlier, this gives us the beginning of day for the
    # required qry_date
    start_date = qry_date + pd.DateOffset(days=-1)
    df = api.qry_holdings(scope, portfolio, start_date)
    check_contents(df)

    # To convert holdings data frame into cash ladder
    # we need to filter out Position types
    df = df[df[TYPE] != "P"].copy()
    check_contents(df)

    # Set date for current balances
    df[SDATE] = pd.to_datetime(df[SDATE].fillna(start_date), utc=True).dt.date

    # Consolidate
    df = df[[CCY, SDATE, TYPE, QTY]].groupby([CCY, SDATE, TYPE], as_index=False).sum()

    # Populate BOD/EOD records

    start_date = start_date.date()  # change form for working with frame data
    # Get unique list of dates, but make sure it includes the qry_date
    dates = pd.concat(
        [df[[SDATE]], pd.DataFrame({SDATE: [qry_date.date()]})], ignore_index=True
    ).drop_duplicates()
    dates = dates[dates[SDATE] > start_date]
    ccys = df[[CCY]].drop_duplicates()

    ccys[JOIN] = 1
    dates[JOIN] = 1
    dates[QTY] = 0
    dates[ORDER] = 1
    dates[TYPE] = "Opening Cash Balance"
    bod = ccys.merge(dates, on=JOIN)
    eod = bod.copy()
    eod[ORDER] = 6
    eod[TYPE] = eod[CCY].str.slice(4) + " Summary"

    df[ORDER] = df[TYPE].map({"C": 2, "A": 3, "R": 4, "F": 5})
    df[TYPE] = df[TYPE].map(
        {
            "C": "Trades to settle",
            "R": "Receivables",
            "A": "Dividends",
            "F": "Forward Fx",
        }
    )

    df = (
        pd.concat([bod, eod, df], ignore_index=True)
        .sort_values([CCY, SDATE, ORDER])
        .reset_index(drop=True)
    )

    # Calculate cumulative quantity
    df[CUM] = df[[CCY, QTY]].groupby([CCY], as_index=False).cumsum()[QTY]

    # Put cumulative balance onto BOD/EOD records
    subset = df[df[ORDER].isin([1, 6])]
    df.loc[subset.index, QTY] = subset[CUM]

    # Filter out T-1 balances (just used to provide BOD balance)

    df = df[df[SDATE] > start_date]

    # Pivot the data
    data = df.set_index([CCY, ORDER, TYPE, SDATE], drop=True).unstack(fill_value=0)

    return data[QTY]


def alt_cash_ladder(api, scope, portfolio, date):
    qry_date = pd.to_datetime(date, utc=True)
    # Run one-day earlier, this gives us the beginning of day for the
    # required qry_date
    start_date = qry_date + pd.DateOffset(days=-1)
    df = api.qry_holdings(scope, portfolio, start_date)

    # filter out Position types
    df = df[df["holding_type"] != "P"]

    df["settlement_date"] = pd.to_datetime(
        df["settlement_date"].fillna(qry_date), utc=True
    ).dt.date
    df = df.sort_values(["security_uid", "settlement_date"])
    df["balance"] = (
        df[["security_uid", "units"]]
        .groupby(["security_uid"], as_index=False)
        .cumsum()["units"]
    )

    columns = [
        "security_uid",
        "settlement_date",
        "commitment",
        "holding_type",
        "commitment_security_uid",
        "units",
        "balance",
    ]

    df = df[columns].rename(
        columns={
            "security_uid": "Currency",
            "settlement_date": "Cash Date",
            "commitment": "Transaction Type",
            "holding_type": "Cash Type",
            "units": "Local Cash Amount",
        }
    )
    return df


def parse(extend=None):
    parser = argparse.ArgumentParser(description="Get Transactions")
    parser.add_argument("scope", help="Scope")
    parser.add_argument("portfolio", help="Portfolio id")
    parser.add_argument("date", metavar="YYYY-MM-DD", help="date")
    parser.add_argument(
        "-f", "--filename", metavar="filename.csv", help="write to this file"
    )
    parser.add_argument(
        "-a", "--alternative", action="store_true", help="alternative view"
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
    if args.alternative:
        df = alt_cash_ladder(api, args.scope, args.portfolio, args.date)
    else:
        df = cash_ladder(api, args.scope, args.portfolio, args.date)

    if args.filename:
        df.to_csv(args.filename)
    else:
        pd.set_option("display.width", None)
        pd.options.display.float_format = "{:,.2f}".format
        pd.set_option("display.max_rows", 1000)
        print(df)


def main():
    process_args(parse())


if __name__ == "__main__":
    import lse

    main()
else:
    from . import lse
