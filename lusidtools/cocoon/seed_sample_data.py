import pandas as pd
from lusidtools.cocoon.cocoon import load_from_data_frame
from lusidtools.cocoon.utilities import load_json_file
import logging
from pathlib import Path

logger = logging.getLogger()

mappings = dict(load_json_file("config/seed_sample_data.json"))


def seed_data(
    api_factory,
    domains,
    scope: str,
    transaction_file: str,
    file_type: str,
    mappings: dict = mappings,
    sub_holding_keys=[],
):

    """
    Description:
    This function allows users to seed their LUSID environment with some core data (e.g. instruments,
    portfolios, and transaction) from one file.

    :param lusid.utilities.ApiClientFactory api_factory: The api factory to use.
    :param list[str] domains: A list of the file_types for upload.
    :param str scope: The scope of the transaction portfolio.
    :param str transaction_file: the absolute or relative path to source file of transaction data.
    :param str file_type: the file extension (e.g. "csv" for "test_transaction.csv".
    :param dict mappings: a file containing mapping of DataFrame headers to LUSID headers.
    :param list sub_holding_keys: a list of sub-holding keys for grouping.
    """

    # Gather a dictionary of supported files

    supported_files = {"csv": "csv", "xlsx": "excel"}

    if Path(transaction_file).suffix != "." + file_type.lower():
        raise ValueError(
            f"""Inconsistent file and file extensions passed: {str(transaction_file)} does 
                not have file extension {file_type}"""
        )

    if file_type not in supported_files:
        raise ValueError(
            f"Unsupported file type, please upload one of the following: {list(supported_files.keys())}"
        )

    data_frame = getattr(pd, f"read_{supported_files[file_type]}")(transaction_file)

    def generic_load_from_data_frame(file_type):

        return (
            load_from_data_frame(
                api_factory=api_factory,
                file_type=file_type,
                scope=scope,
                data_frame=data_frame,
                mapping_required=mappings[file_type]["required"],
                mapping_optional=mappings[file_type]["optional"],
                identifier_mapping=mappings[file_type]["identifier_mapping"],
                property_columns=mappings[file_type]["properties"],
                properties_scope=scope,
                sub_holding_keys=sub_holding_keys,
            ),
        )

    overall_results = {}

    for domain in domains:

        if domain not in mappings:
            raise ValueError(
                f"The provided file_type of {domain} has no associated mapping"
            )

        logging.info(f"Loading {domain} DataFrame into LUSID...")

        response = generic_load_from_data_frame(domain)

        logging.info(f"Loading of {domain} is COMPLETED")

        overall_results[domain] = response

    return overall_results
