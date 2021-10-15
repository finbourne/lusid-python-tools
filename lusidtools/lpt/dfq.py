import argparse
import re
import pandas as pd


def parse(with_inputs=True, args=None):
    parser = argparse.ArgumentParser(
        description="DataFrame Query Tool", fromfile_prefix_chars="@"
    )
    if with_inputs:
        parser.add_argument("input", nargs="+", help="source csv file")
    parser.add_argument("-c", "--columns", action="store_true", help="display columns")
    parser.add_argument(
        "-s", "--select", nargs="*", metavar="column", help="fields to select"
    )
    parser.add_argument(
        "-w",
        "--where",
        nargs="*",
        metavar="column=value",
        help="filtering eg. -w 'cost>100' 'cost>-100' or -w "
        "'strat=Tech,Pharma' 'region!=UK'",
    )
    parser.add_argument(
        "-o", "--order", nargs="*", metavar="column", help="fields to sort by"
    )
    parser.add_argument(
        "-u", "--unique", action="store_true", help="unique values only"
    )
    parser.add_argument(
        "-g", "--groupby", nargs="*", metavar="column", help="fields to groupby"
    )
    parser.add_argument("--filename", metavar="filename", help="save output to file")
    parser.add_argument(
        "-f", "--first", type=int, default=0, help='show first "n" records'
    )
    parser.add_argument(
        "--strings", action="store_true", help="interpret all fields as strings"
    )
    parser.add_argument(
        "-l", "--last", type=int, default=0, help='show last "n" records'
    )
    parser.add_argument(
        "-j", "--join", nargs="+", help="join to other frame. path, criterion"
    )
    parser.add_argument("--dp", type=int, default=2)
    parser.add_argument("-t", "--transpose", action="store_true")
    parser.add_argument("-m", action="store_true")
    parser.add_argument("-i", "--index", action="store_true")
    parser.add_argument("-x", "--xls", action="store_true")
    parser.add_argument("--glob", action="store_true")
    parser.add_argument("--identify", action="store_true")
    parser.add_argument("--separator", help="separator from text files")
    parser.add_argument(
        "--tab", action="store_true", help="tab separator for text files"
    )
    parser.add_argument(
        "--latin", action="store_true", help="read files with latin-1 encoding"
    )
    parser.add_argument(
        "--markdown", action="store_true", help="give output in markdown format"
    )
    parser.add_argument("--count", action="store_true", help="count records in groups")
    parser.add_argument(
        "--single", nargs="+", help="Single column uniqueness constraint."
    )

    return parser.parse_args(args)


def apply_args(args, given_df):
    nrows = (
        args.first
        if args.first > 0
        and args.last == 0
        and given_df is None
        and args.groupby is None
        and args.where is None
        and args.order is None
        and len(args.input) == 1
        else None
    )

    if args.glob:
        import glob

        args.input = glob.glob(args.input[0])

    if args.columns and given_df is None:
        args.input = args.input[0:1]
        nrows = 2

    reader_args = {"nrows": nrows}
    if args.strings:
        reader_args["dtype"] = str

    if args.tab:
        reader_args["sep"] = "\t"
    elif args.separator:
        reader_args["sep"] = args.separator

    if args.latin:
        reader_args["encoding"] = "latin-1"

    def load_frame(path):
        if path.endswith(".pk"):
            return pd.read_pickle(path)
        elif ".xls" in path.lower():
            s = path.split(":")
            if len(s) == 2:
                return pd.read_excel(
                    s[0], engine="openpyxl", **reader_args, sheet_name=s[1]
                )
            return pd.read_excel(path, engine="openpyxl", **reader_args)
        else:
            return pd.read_csv(path, **reader_args)

    if given_df is not None:
        dfs = [("Given", given_df)]
    else:
        dfs = [(fn, load_frame(fn)) for fn in args.input]

    if args.identify:
        for fn, df in dfs:
            df["FILE-NAME"] = fn
            df["FILE-INDEX"] = df.index

    if len(dfs) == 1:
        df = dfs[0][1]
    else:
        df = pd.concat([d[1] for d in dfs], ignore_index=True, sort=False)
        del dfs

    if args.columns:
        return df

    if args.join:
        cols = [c.split("=") for c in args.join[1:]]
        df = df.merge(
            load_frame(args.join[0]),
            how="left",
            left_on=[c[0] for c in cols],
            right_on=[c[-1] for c in cols],
            indicator=True,
        )

    if args.where:
        for c in args.where:

            # Get keys, values and operations
            kv = re.findall(r"[^>,<,=]+", c)
            ops = re.findall(r"[>,<,=]", c)

            if len(kv) < 2:
                raise ValueError(f"No keys or values found in clause: '{c}'")

            # define behaviour
            EQ = 1
            GT = 2
            LT = 4
            GE = EQ + GT  # 3
            LE = EQ + LT  # 5

            op = EQ if "=" in ops else 0
            op += GT if ">" in ops else 0
            op += LT if "<" in ops else 0

            col = kv[0]
            invert = col.endswith("!")

            if invert:
                col = col[:-1]

            if kv[1].startswith("IN:") and kv[1].endswith(".csv"):
                s = set(pd.read_csv(kv[1][3:]).iloc[:, 0].astype(str))
            else:
                s = kv[1:]

            if len(s) == 1:
                if "*" in kv[1]:
                    crit = (
                        df[col]
                        .astype(str)
                        .fillna("")
                        .str.match("({})".format(kv[1].replace("*", ".*")))
                    )
                else:
                    dflt = ""
                    v = kv[1]
                    if v.endswith(" as int"):
                        v = int(v[:-7])
                        dflt = 0
                    elif df[col].dtype == int:
                        v = int(v)
                        dflt = 0
                    elif df[col].dtype == float:
                        v = float(v)
                        dflt = 0.0

                    # apply appropriate boolean operator to get filter mask
                    crit = {
                        EQ: lambda f, v: f == v,
                        GT: lambda f, v: f > v,
                        LT: lambda f, v: f < v,
                        GE: lambda f, v: f >= v,
                        LE: lambda f, v: f <= v,
                    }.get(op, lambda f, v: print("Invalid operation!"))(
                        df[col].fillna(dflt), v
                    )

            else:
                crit = df[col].isin(s)

            # apply filter mask
            if invert:
                df = df[~crit]
            else:
                df = df[crit]

    if args.groupby:
        if args.count:
            df = df.groupby(args.groupby, as_index=False).size().reset_index()
        else:
            df = df.groupby(args.groupby, as_index=False).sum()

    if args.select:
        if len(args.select) == 1 and args.select[0].startswith("file:"):
            args.select = [
                col.replace("\n", "")
                for col in open(args.select[0][5:], "r").readlines()
            ]
        df = df[args.select]

    if args.unique:
        df = df.drop_duplicates()

    if args.single:
        df = df.drop_duplicates(args.single)

    if args.order:
        df = df.sort_values(args.order)

    return df


def dfq(args, given_df=None):
    if (given_df is not None) and (not isinstance(given_df, pd.DataFrame)):
        print(given_df)
        exit(0)

    df = apply_args(args, given_df)

    if args.columns:
        print("\n".join(df.columns.values))
        exit()

    # Display a dataframe with no cropping
    def display_df(df, decimals=2):
        if args.xls:
            import xlwings as xw

            wb = xw.Book()
            wb.sheets[0].range("A1").options(index=args.index).value = df
        else:
            fmt = "{:,." + str(decimals) + "f}"
            pd.options.display.float_format = fmt.format
            pd.set_option("display.max_colwidth", 200)

            try:
                if args.transpose:
                    df = df.T
                    args.index = True
                with pd.option_context("display.width", None, "display.max_rows", 1000):
                    if args.markdown:
                        print(
                            df.fillna("").to_markdown(
                                index=args.index, floatfmt=f".{decimals}f"
                            )
                        )
                    else:
                        print(df.fillna("").to_string(index=args.index))
            except:
                print(df.to_string(index=args.index))

    def display(df, subset=None, total=0):
        if args.filename:
            if subset:
                filename = args.filename.replace(".", "-{}-{}.".format(subset, len(df)))
            else:
                filename = args.filename
            if filename.lower().endswith(".xlsx"):
                df.to_excel(filename, index=False, freeze_panes=(1, 0))
            elif filename.lower().endswith(".pk"):
                df.to_pickle(filename)
            else:
                df.to_csv(filename, index=False)
        else:
            if subset:
                print("{} {}".format(subset, len(df)))
            display_df(df, args.dp)

    if args.first > 0:
        display(df[: args.first], "First")

    if args.last > 0:
        display(df[-args.last :], "Last")

    if args.first == 0 and args.last == 0:
        display(df)


def main():
    dfq(parse())


if __name__ == "__main__":
    main()
