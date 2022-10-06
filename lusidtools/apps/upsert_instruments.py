import logging
import sys
from lusid.utilities import ApiClientFactory
from lusidtools.cocoon import (
    load_from_data_frame,
    load_data_to_df_and_detect_delimiter,
    check_mapping_fields_exist,
    parse_args,
    validate_mapping_file_structure,
    identify_cash_items,
    load_json_file,
    cocoon_printer,
)
from lusidtools.logger import LusidLogger


def load_instruments(args):
    file_type = "instruments"

    # create ApiFactory
    factory = ApiClientFactory(api_secrets_filename=args["secrets_file"])

    # get data
    if args["delimiter"]:
        logging.info(f"delimiter specified as {repr(args['delimiter'])}")
    logging.debug("Getting data")
    instruments = load_data_to_df_and_detect_delimiter(args)

    # get mappings
    mappings = load_json_file(args["mapping"])

    if "property_columns" in mappings[file_type].keys() and not args["scope"]:
        err = (
            r"properties must be upserted to a specified scope, but no scope was provided. "
            r"Please state what scope to upsert properties to using '-s'."
        )
        logging.error(err)
        raise ValueError(err)

    validate_mapping_file_structure(mappings, instruments.columns, file_type)
    if "cash_flag" in mappings.keys():
        instruments, mappings = identify_cash_items(
            instruments, mappings, file_type, True
        )

    if args["dryrun"]:
        logging.info("--dryrun specified as True, exiting before upsert call is made")
        return 0

    instruments_response = load_from_data_frame(
        api_factory=factory,
        data_frame=instruments,
        scope=args["scope"],
        properties_scope=args.get("property_scope", args["scope"]),
        mapping_required=mappings[file_type]["required"],
        mapping_optional=mappings[file_type].get("optional", {}),
        file_type=file_type,
        identifier_mapping=mappings[file_type]["identifier_mapping"],
        batch_size=args["batch_size"],
        property_columns=mappings[file_type].get("property_columns", []),
    )

    succ, errors, failed = cocoon_printer.format_instruments_response(
        instruments_response
    )
    logging.info(f"number of successful upserts: {len(succ)}")
    logging.info(f"number of failed upserts    : {len(failed)}")
    logging.info(f"number of errors            : {len(errors)}")

    if args["display_response_head"]:
        logging.info(succ.head(40))
        logging.info(errors.head(40))
        logging.info(failed.head(40))

    return instruments_response


def main():
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"], args["logging_file"])
    load_instruments(args)

    return 0


if __name__ == "__main__":
    main()
