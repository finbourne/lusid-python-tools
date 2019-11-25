import json
import logging
import os
import sys
from lusid.utilities import ApiClientFactory
from lusidtools.cocoon import (
    load_from_data_frame,
    load_data_to_df_and_detect_delimiter,
    check_mapping_fields_exist,
    parse_args,
    validate_mapping_file_structure,
    identify_cash_items,
)
from lusidtools.logger import LusidLogger


def load_mapping_file_for_file_type(mapping_path, file_type) -> dict:
    """
    :param mapping_path: The full path of mapping_file.json
    :param file_type: The type of file i.e. transactions or holdings or instruments
    :return:
    """
    if not os.path.exists(mapping_path):
        raise OSError(f"mapping file not found at {mapping_path}")
    with open(mapping_path, "r") as read_file:
        return json.load(read_file)


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
    mappings = load_mapping_file_for_file_type(args["mapping"], file_type)

    if "property_columns" not in mappings.keys() and not args["scope"]:
        raise ValueError(
            r"Instrument properties must be upserted to a specified scope, but no scope was provided. "
            r"Please state what scope to upsert properties to using '-s'."
        )

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
        mapping_required=mappings[file_type]["required"],
        mapping_optional=mappings[file_type]["optional"],
        file_type=file_type,
        identifier_mapping=mappings[file_type]["identifier_mapping"],
        batch_size=args["batch_size"],
        property_columns=mappings[file_type]["property_columns"]
        if "property_columns" in mappings[file_type].keys()
        else [],
    )

    total_success = sum(
        [
            len(resp.values.keys())
            for resp in instruments_response["instruments"]["success"]
        ]
    )
    total_failed = sum(
        [
            len(resp.failed.keys())
            for resp in instruments_response["instruments"]["success"]
        ]
    )
    logging.info(f"Success: {total_success}/{len(instruments)}")
    logging.info(f"Fail:    {total_failed}/{len(instruments)}")
    return instruments_response


def main(argv):
    args, ap = parse_args(sys.argv[1:])
    LusidLogger(args["debug"])
    load_instruments(args)

    return 0


if __name__ == "__main__":
    main(sys.argv)
