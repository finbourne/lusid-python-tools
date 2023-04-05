import sys
import logging
from lusid.utilities import ApiClientFactory

from lusidtools.cocoon import (
    load_data_to_df_and_detect_delimiter,
    load_from_data_frame,
    parse_args,
    identify_cash_items,
    validate_mapping_file_structure,
    load_json_file,
    cocoon_printer,
)
from lusidtools.logger import LusidLogger


def load_transactions(args):
    file_type = "transactions"

    factory = ApiClientFactory(api_secrets_filename=args["secrets_file"])

    if args["delimiter"]:
        logging.info(f"delimiter specified as {repr(args['delimiter'])}")
    logging.debug("Getting data")
    transactions = load_data_to_df_and_detect_delimiter(args)

    mappings = load_json_file(args["mapping"])

    if "cash_flag" in mappings.keys():
        identify_cash_items(transactions, mappings, file_type)

    validate_mapping_file_structure(mappings, transactions.columns, file_type)

    if args["dryrun"]:
        logging.info("--dryrun specified as True, exiting before upsert call is made")
        return 0

    transactions_response = load_from_data_frame(
        api_factory=factory,
        data_frame=transactions,
        scope=args["scope"],
        properties_scope=args.get("property_scope", args["scope"]),
        identifier_mapping=mappings[file_type]["identifier_mapping"],
        mapping_required=mappings[file_type]["required"],
        mapping_optional=mappings[file_type].get("optional", {}),
        file_type=file_type,
        batch_size=args["batch_size"],
        property_columns=mappings[file_type].get("property_columns", []),
    )

    # print_response(transactions_response, file_type)
    succ, errors = cocoon_printer.format_transactions_response(transactions_response)

    logging.info(f"number of successful upserts: {len(succ)}")
    logging.info(f"number of errors            : {len(errors)}")

    if args["display_response_head"]:
        logging.info(succ.head(40))
        logging.info(errors.head(40))

    return transactions_response


def main():
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"], args["logging_file"])
    load_transactions(args)

    return 0


if __name__ == "__main__":
    main()
