import logging
import sys
from lusid.utilities import ApiClientFactory

from lusidtools.logger import LusidLogger
from lusidtools.cocoon import (
    parse_args,
    load_data_to_df_and_detect_delimiter,
    load_from_data_frame,
    validate_mapping_file_structure,
    load_json_file,
    cocoon_printer,
)


def load_portfolios(args):
    file_type = "portfolios"

    factory = ApiClientFactory(api_secrets_filename=args["secrets_file"])

    if args["delimiter"]:
        logging.info(f"delimiter specified as {repr(args['delimiter'])}")
    logging.debug("Getting data")
    portfolios = load_data_to_df_and_detect_delimiter(args)

    mappings = load_json_file(args["mapping"])

    validate_mapping_file_structure(mappings, portfolios.columns, file_type)

    if args["dryrun"]:
        logging.info("--dryrun specified as True, exiting before upsert call is made")
        return 0

    portfolios_response = load_from_data_frame(
        api_factory=factory,
        data_frame=portfolios,
        scope=args["scope"],
        properties_scope=args.get("property_scope", args["scope"]),
        mapping_required=mappings[file_type]["required"],
        mapping_optional=mappings[file_type].get("optional", {}),
        file_type=file_type,
        batch_size=args["batch_size"],
        property_columns=mappings[file_type].get("property_columns", []),
        sub_holding_keys=mappings[file_type].get("sub_holding_keys", []),
    )

    succ, errors = cocoon_printer.format_portfolios_response(portfolios_response)

    logging.info(f"number of successful upserts: {len(succ)}")
    logging.info(f"number of errors            : {len(errors)}")

    if args["display_response_head"]:
        logging.info(succ.head(40))
        logging.info(errors.head(40))

    return portfolios_response


def main():
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"], args["logging_file"])
    load_portfolios(args)

    return 0


if __name__ == "__main__":
    main()
