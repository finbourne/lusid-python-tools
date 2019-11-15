#!/usr/bin/python3
import pandas as pd
from lusidtools.lpt import stdargs
from lusidtools.lpt import lse
from lusidtools.lpt import lpt

args = stdargs.Parser(
    "Delete Portfolio Properties", ["scope", "portfolio", "properties"]
).parse()
api = lse.connect(args)

api.call.delete_portfolio_properties(
    scope=args.scope, code=args.portfolio, portfolio_property_keys=args.properties
).match(left=lpt.display_error, right=lambda r: print("Done!"))
