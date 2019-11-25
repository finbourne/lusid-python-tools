import logging
import sys
from lusid.utilities import ApiClientFactory

from lusidtools.apps.upsert_instruments import load_mapping_file_for_file_type
from lusidtools.logger import LusidLogger
from lusidtools.cocoon import (
    parse_args,
    load_data_to_df_and_detect_delimiter,
    load_from_data_frame,
    identify_cash_items,
    validate_mapping_file_structure,
)


def load_holdings(args):
    file_type = "holdings"

    factory = ApiClientFactory(api_secrets_filename=args["secrets_file"])

    if args["delimiter"]:
        logging.info(f"delimiter specified as {repr(args['delimiter'])}")
    logging.debug("Getting data")
    holdings = load_data_to_df_and_detect_delimiter(args)

    mappings = load_mapping_file_for_file_type(args["mapping"], file_type)
    if "cash_flag" in mappings.keys():
        holdings, mappings = identify_cash_items(holdings, mappings, file_type)

    validate_mapping_file_structure(mappings, holdings.columns, file_type)

    if args["dryrun"]:
        logging.info("--dryrun specified as True, exiting before upsert call is made")
        return 0

    holdings_response = load_from_data_frame(
        api_factory=factory,
        data_frame=holdings,
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

    total_success = len(holdings_response["holdings"]["success"])
    total_failed = len(holdings_response["holdings"]["errors"])
    logging.info(f"Success: {total_success}/{total_success + total_failed}")
    logging.info(f"Fail:    {total_failed}/{total_success + total_failed}")
    return holdings_response


def main(argv):
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"])
    load_holdings(args)

    return 0


if __name__ == "__main__":
    main(sys.argv)
