from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import txn_config_yaml as tcy

TOOLNAME = "txn_cfg"
TOOLTIP = "Get/Set the transaction configuration"


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Get/Set transaction configuration", ["filename", "limit", "NODFQ"]
        )
        .add(
            "action",
            choices=("get", "set", "try"),
            help="get or set the config. 'Try' can be used to validate a custom encoding",
        )
        .add("--raw", action="store_true", help="use raw (non custom) encoding")
        .add("--json", action="store_true", help="display the json to be sent")
        .add("--group", action="store_true", help="set a single group")
        .add(
            "--force",
            action="store_true",
            help="set a single group, remove existing aliases for the group",
        )
        .extend(extend)
        .parse(args)
    )


def validate_group(txn_types, group):
    for txn in txn_types:
        for alias in txn.aliases:
            assert alias.transaction_group == group, "More than one group in the list"


def rem_groups(txn_types_old, group, arg):
    def still_valid(tt):
        for cand in tt.aliases:
            if cand.transaction_group != group:
                return True
        if arg is not True:
            raise AssertionError(
                "Existing group detected, use '--force' to remove them"
            )
        return False

    def clear_out_group(tt):
        check = len(tt.aliases)
        tt.aliases = [cand for cand in tt.aliases if cand.transaction_group != group]

        if len(tt.aliases) != check and arg != True:
            raise AssertionError(
                "Existing group detected, use '--force' to remove them"
            )
        return tt

    return [clear_out_group(t) for t in txn_types_old if still_valid(t)]


def merge_sets(txn_types_old, txn_types, arg):
    group = txn_types[0].aliases[0].transaction_group

    validate_group(txn_types, group)
    txn_types_clean = rem_groups(txn_types_old, group, arg)

    txn_types += txn_types_clean
    return txn_types


def process_args(api, args):
    y = tcy.TxnConfigYaml(api.models)

    if args.action == "get":

        def get_success(result):
            y.dump(
                y.TransactionSetConfigurationDataNoLinks(
                    result.content.transaction_configs, result.content.side_definitions
                ),
                args.filename,
                args.raw,
            )
            return None

        return api.call.list_configuration_transaction_types().bind(get_success)

    if args.action == "try":
        ffs = y.load(args.filename)
        y.dump(ffs, "{}-try".format(args.filename))

    if args.action == "set":

        def set_success(result):
            print(y.get_yaml(result.content))
            return None

        if args.group:
            txn_types = y.load(args.filename)

            result = api.call.list_configuration_transaction_types()

            if result.right is not None:
                txn_types_old = result.right.content
            else:
                raise ValueError("Api call did not return correct result")

            txn_types = y.load_update_str(
                y.get_yaml(merge_sets(txn_types_old, txn_types, args.force))
            )
        else:
            txn_types = y.load_update(args.filename)

        # y.dump(ffs,"{}-set".format(args.filename),True)
        if args.json:
            print(txn_types)
            return None
        else:
            return api.call.set_configuration_transaction_types(
                transaction_set_configuration_data_request=txn_types
            ).bind(set_success)


# Standalone tool
def main(parse=parse):
    lpt.standard_flow(parse, lse.connect, process_args)
