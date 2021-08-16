import lusid
import pandas as pd
from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs

TOOLNAME = "rec"
TOOLTIP = "Query Holdings Reconciliation"

AGG_INSTR = "Instrument/default/Name"
AGG_UID = "Instrument/default/LusidInstrumentId"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Get Holdings Reconciliation", ["filename", "limit"])
        .add("scope_left", help="Scope - left")
        .add("portfolio_left", help="Portfolio id - left")
        .add("date_left", nargs="+", metavar="YYYY-MM_DD")
        .add("scope_right", help="Scope - right")
        .add("portfolio_right", help="Portfolio id - right")
        .add("date_right", nargs="+", metavar="YYYY-MM_DD")
        .add("-m", "--monitor", action="store_true", help="monitor the run")
        .add("--group", action="store_true", help="group instead of portfolio")
        .extend(extend)
        .parse(args)
    )


def run_query(
    api,
    args,
    scope_left=None,
    portfolio_left=None,
    date_left=None,
    scope_right=None,
    portfolio_right=None,
    date_right=None,
    instr_props=[],
):
    # Create request for left
    left_id = lusid.models.ResourceId(scope=scope_left, code=portfolio_left)

    left = api.models.PortfolioReconciliationRequest(
        portfolio_id=left_id, effective_at=lpt.to_date(date_left)
    )

    # create request for right
    right_id = lusid.models.ResourceId(scope=scope_right, code=portfolio_right)

    right = api.models.PortfolioReconciliationRequest(
        portfolio_id=right_id, effective_at=lpt.to_date(date_right)
    )

    # form reconciliation request
    request = api.models.PortfoliosReconciliationRequest(
        left=left, right=right, instrument_property_keys=instr_props + [AGG_INSTR],
    )

    return api.call.reconcile_holdings(portfolios_reconciliation_request=request).bind(
        parse_reconciled_holdings
    )


def process_args(api, args):
    return run_query(
        api=api,
        args=args,
        scope_left=args.scope_left,
        portfolio_left=args.portfolio_left,
        date_left=args.date_left[0],
        scope_right=args.scope_right,
        portfolio_right=args.portfolio_right,
        date_right=args.date_right[0],
    )  # .bind(lambda x: x[1])


def parse_reconciled_holdings(result):
    if len(result.content.values) == 0:
        return "No reconciliation breaks"

    def parse_breaks(result):
        data = []
        for item in result.content.values:
            row = {
                "LUID": item.instrument_uid,
                "Name": [
                    i.value.label_value if i.key == AGG_INSTR else None
                    for i in item.instrument_properties
                ][0],
                "diff_cost": item.difference_cost.amount,
                "diff_cost_ccy": item.difference_cost.currency,
                "left_cost": item.left_cost.amount,
                "left_cost_ccy": item.left_cost.currency,
                "left_units": item.left_units,
                "right_cost": item.right_cost.amount,
                "right_cost_ccy": item.right_cost.currency,
                "right_units": item.right_units,
            }

            shks = {k: v.value.label_value for k, v in item.sub_holding_keys.items()}
            row.update(shks)
            data.append(row)
        df = pd.DataFrame(data)
        df.fillna("N/A", inplace=True)

        return df

    return parse_breaks(result)


def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
