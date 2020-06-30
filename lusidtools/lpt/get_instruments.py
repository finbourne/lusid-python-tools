import pandas as pd

from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TOOLNAME = "instr"
TOOLTIP = "Display specified Instruments"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Instruments", ["filename", "limit", "asat"])
        .add("instrument", nargs="*")
        .add("--type", "-t", default="ClientInternal", help="Instrument type")
        .add("--identifiers", nargs="*", help="identifiers to display")
        .add("--properties", nargs="*", help="properties required for display")
        .add(
            "--from",
            dest="from_file",
            nargs=2,
            metavar=("filename.csv", "column"),
            help="load values from file",
        )
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    MAX_PROPS = 50

    def success(result):
        idents = [f"identifiers.KEY:{v}" for v in args.identifiers]
        columns = ["lusid_instrument_id", "name"]
        columns.extend(idents)
        if args.properties:
            columns.extend(["P:" + v for v in args.properties])

        df = lpt.to_df(result.content.values.values(), columns)
        return df.rename(columns=dict(zip(idents, args.identifiers)))

    if args.from_file:
        df = pd.read_csv(args.from_file[0])[args.from_file[1:]].drop_duplicates()
        args.instrument.extend(df[args.from_file[1]].values)

    def step3(result, props_remaining, final_dataframe):
        df = success(result)

        if final_dataframe is None:
            final_dataframe = df
        else:
            props = [c for c in df.columns.values if c.startswith("P:")]
            final_dataframe = final_dataframe.merge(
                df[["lusid_instrument_id"] + props],
                how="left",
                on="lusid_instrument_id",
            )

        if len(props_remaining) > 0:
            args.properties = props_remaining[:MAX_PROPS]
            remaining = props_remaining[MAX_PROPS:]
            return api.call.get_instruments(
                args.type, request_body=args.instrument, property_keys=args.properties
            ).bind(lambda r: step3(r, remaining, final_dataframe))
        return final_dataframe

    def step2(result=None):
        next_step = success
        if result is not None:
            # Contains full set of instrument properties
            l = list(
                result.apply(lambda r: f"Instrument/{r['scope']}/{r['code']}", axis=1)
            )
            args.properties = l[:MAX_PROPS]  # limitation
            if len(l) > MAX_PROPS:
                next_step = lambda r: step3(r, l[MAX_PROPS:], None)

        return api.call.get_instruments(
            args.type, request_body=args.instrument, property_keys=args.properties
        ).bind(next_step)

    if args.identifiers is None:
        args.identifiers = [args.type]

    if args.properties:
        if args.properties[0] == "all":
            from lusidtools.lpt import qry_properties as qp

            return qp.process_args(api, qp.parse(args=["-d", "Instrument"])).bind(step2)

    return step2()


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
