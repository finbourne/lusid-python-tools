import pandas as pd

from tqdm import tqdm
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TOOLNAME = "prop_create"
TOOLTIP = "Create simple property definitions"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Create property definitions", ["filename", "quiet", "NODFQ"])
        .add("--property", nargs="+")
        .add("--type", default="Label", choices=["Label", "Metric"])
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    res_type = {"Metric": "number", "Label": "string"}

    if args.filename:
        requests = api.from_df(
            pd.read_csv(args.input), api.models.CreatePropertyDefinitionRequest
        )
    elif args.property:
        requests = [
            api.models.CreatePropertyDefinitionRequest(
                domain=p[0],
                scope=p[1],
                code=p[2],
                value_required=False,
                display_name=p[2],
                life_time="Perpetual",
                data_type_id=api.models.ResourceId("system", res_type[args.type]),
            )
            for p in map(lambda v: v.split("/"), args.property)
        ]

    with tqdm(
        requests, disable=args.quiet or len(requests) < 2, total=len(requests)
    ) as iter:
        for prop in iter:
            iter.set_description(prop.display_name)
            result = api.call.create_property_definition(
                create_property_definition_request=prop
            )
            if result.left is not None:
                return result


# Standalone tool
def main():
    lpt.standard_flow(parse, lse.connect, process_args)
