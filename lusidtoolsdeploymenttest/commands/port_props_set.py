#!/usr/bin/python3

import lusid.models as models
from lusidtools.lpt import stdargs
from lusidtools.lpt import lse
from lusidtools.lpt import lpt

args = (
    stdargs.Parser("Set Portfolio Properties", ["scope", "portfolio"])
    .add("property")
    .add("value")
    .add("--date")
    .add("--metric", action="store_true")
    .add("--test", action="store_true")
    .parse()
)
api = lse.connect(args)


def metric(m):
    return api.models.PropertyValue(metric_value=models.MetricValue(value=float(m)))


def label(l):
    return api.models.PropertyValue(label_value=str(l))


property_request = {
    args.property: api.models.ModelProperty(
        key=args.property,
        value=metric(args.value) if args.metric else label(args.value),
        effective_from=lpt.to_date(args.date) if args.date else None,
    )
}

if args.test:
    print(property_request)
else:
    api.call.upsert_portfolio_properties(
        scope=args.scope, code=args.portfolio, portfolio_properties=property_request
    ).match(left=lpt.display_error, right=lambda r: print("Done!"))
