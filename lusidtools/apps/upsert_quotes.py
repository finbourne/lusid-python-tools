import logging
import sys
import lusid
from lusid.utilities import ApiClientFactory
from lusidtools.cocoon import (
    load_from_data_frame,
    load_data_to_df_and_detect_delimiter,
    check_mapping_fields_exist,
    parse_args,
    validate_mapping_file_structure,
    identify_cash_items,
    load_json_file,
    scale_quote_of_type,
    cocoon_printer,
)
from lusidtools.logger import LusidLogger


def load_quotes(args):
    file_type = "quotes"

    # create ApiFactory
    factory = ApiClientFactory(api_secrets_filename=args["secrets_file"])

    # get data
    if args["delimiter"]:
        logging.info(f"delimiter specified as {repr(args['delimiter'])}")
    logging.debug("Getting data")
    quotes = load_data_to_df_and_detect_delimiter(args)

    # get mappings
    mappings = load_json_file(args["mapping"])

    # check properties exist
    if "property_columns" in mappings[file_type].keys() and not args["scope"]:
        err = (
            r"properties must be upserted to a specified scope, but no scope was provided. "
            r"Please state what scope to upsert properties to using '-s'."
        )
        logging.error(err)
        raise ValueError(err)

    if "cash_flag" in mappings.keys():
        quotes, mappings = identify_cash_items(quotes, mappings, "quotes", True)

    validate_mapping_file_structure(mappings, quotes.columns, file_type)

    if "quote_scalar" in mappings[file_type].keys():
        quotes, mappings = scale_quote_of_type(quotes, mappings)

    if args["dryrun"]:
        return quotes

    quotes_response = load_from_data_frame(
        api_factory=factory,
        data_frame=quotes,
        scope=args["scope"],
        properties_scope=args.get("property_scope", args["scope"]),
        identifier_mapping={},
        mapping_required=mappings[file_type]["required"],
        mapping_optional=mappings[file_type].get("optional", {}),
        file_type=file_type,
        batch_size=args["batch_size"],
        property_columns=mappings[file_type].get("property_columns", []),
    )
    succ, errors, failed = cocoon_printer.format_quotes_response(quotes_response)
    logging.info(f"number of successful upserts: {len(succ)}")
    logging.info(f"number of failed upserts    : {len(failed)}")
    logging.info(f"number of errors            : {len(errors)}")

    if args["display_response_head"]:
        logging.info(succ.head(40))
        logging.info(errors.head(40))
        logging.info(failed.head(40))

    return quotes_response


def main():
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"], args["logging_file"])
    load_quotes(args)

    return 0


if __name__ == "__main__":
    main()
