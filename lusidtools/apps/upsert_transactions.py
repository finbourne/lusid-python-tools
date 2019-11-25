import sys
import logging
from lusid.utilities import ApiClientFactory

from lusidtools.apps.upsert_instruments import load_mapping_file_for_file_type
from lusidtools.cocoon import (
    load_data_to_df_and_detect_delimiter,
    load_from_data_frame,
    parse_args,
    identify_cash_items,
    validate_mapping_file_structure,
)
from lusidtools.logger import LusidLogger


def load_transactions(args):
    file_type = "transactions"

    factory = ApiClientFactory(api_secrets_filename=args["secrets_file"])

    if args["delimiter"]:
        logging.info(f"delimiter specified as {repr(args['delimiter'])}")
    logging.debug("Getting data")
    transactions = load_data_to_df_and_detect_delimiter(args)

    mappings = load_mapping_file_for_file_type(args["mapping"], file_type)

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
        identifier_mapping=mappings[file_type]["identifier_mapping"],
        mapping_required=mappings[file_type]["required"],
        mapping_optional=mappings[file_type]["optional"],
        file_type=file_type,
        batch_size=args["batch_size"],
        property_columns=mappings[file_type]["property_columns"]
        if "property_columns" in mappings[file_type].keys()
        else [],
    )

    # print_response(transactions_response, file_type)
    total_success = len(transactions_response["transactions"]["success"])
    total_failed = len(transactions_response["transactions"]["errors"])
    logging.info(f"Success: {total_success}/{total_success + total_failed}")
    logging.info(f"Fail:    {total_failed}/{total_success + total_failed}")
    return transactions_response


def main(argv):
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"])
    load_transactions(args)

    return 0


if __name__ == "__main__":
    main(sys.argv)
