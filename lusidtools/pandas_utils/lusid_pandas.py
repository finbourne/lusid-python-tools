import pandas as pd
from flatten_json import flatten
import logging

logger = logging.getLogger()


def lusid_response_to_data_frame(
    lusid_response, rename_properties: bool = False, column_name_mapping: dict = None
):
    """
    This function takes a LUSID API response and attempts to convert the response into a Pandas DataFrame or Series.
    Not all LUSID API responses are the same datatype. Therefore we need to implement some if/else conditional logic to
    see how the response should be converted.
    In terms of an implementation, the function checks for two attributes on the response:
    (1) A "values" attribute - these are lists of LUSID objects. Example: the "Get Holdings" response has a values
    attribute which contains a list of individual holding objects.
    (2) A "to_dict" attribute - this is one dictionary of values.

    Parameters
    ----------
    lusid_response
        a response from the LUSID APIs (e.g. VersionedResourceListOfPortfolioHolding)
    rename_properties : bool
        this parameter formats the returned DataFrame.
        Specifically, the formatter does two things; (1) removes any metadata columns for properties and SHKs, and (2)
        simplifies the naming of column headers for properties and SHKs.
    rename_mapping : dict
        a dictionary which is used to map old column headers to new ones.
        The dictionary key is old header while the dictionary value in new header. If key does not exist, the function
        will ignore the non-existant mapping.

    Returns
    -------

    pandas.DataFrame
    """

    # Check if lusid_response is a list of the same objects which all have to_dict() method

    if type(lusid_response) == list and len(lusid_response) == 0:
        return pd.DataFrame()

    elif type(lusid_response) == list and len(lusid_response) > 0:

        first_item_type = type(lusid_response[0])

        if not all(isinstance(x, first_item_type) for x in lusid_response):
            raise TypeError("All items in list must be of the same data type")

        if not hasattr(first_item_type, "to_dict"):
            raise TypeError("All object items in list must have a to_dict() method")

        response_df = pd.DataFrame(
            flatten(value.to_dict(), ".") for value in lusid_response
        )

    # Check if lusid_response has a values attribute with data type of list

    elif hasattr(lusid_response, "values") and type(lusid_response.values) == list:

        response_df = pd.DataFrame(
            flatten(value.to_dict(), ".") for value in lusid_response.values
        )

    # Check if response object has to_dict() method

    elif hasattr(lusid_response, "to_dict"):

        response_df = pd.DataFrame.from_dict(
            (flatten(lusid_response.to_dict(), ".")),
            orient="index",
            columns=["response_values"],
        )
    else:

        raise TypeError(
            """Cannot map response object to pandas DataFrame or Series. The LUSID response object must have
                        either the values attribute or the to_dict() method, or be a list of objects with 
                        the to_dict() method"""
        )

    if rename_properties:

        # Collect columns to drop - these are meta-data columns for properties and sub-holding keys
        columns_to_drop = list(
            response_df.filter(
                regex="(^sub_holding_keys|^properties).*(key$|metric_value$|effective_from$)"
            ).columns
        )

        response_df.drop(columns_to_drop, axis=1, inplace=True)

        # Rename the properties and sub-holding keys column to show the property "code" only
        # Recall the format is "domain/scope/code"

        rename_dict = {}

        columns_to_rename = response_df.filter(
            regex="(^sub_holding_keys|^properties).*(label_value$|value.metric_value.value$)"
        ).columns

        for column in columns_to_rename:

            # find the property code substring
            # The property code should always be displayed after the second "/"
            # Example: sub_holding_keys.Transaction/MultiAssetScope/PortionSubClass.value.label_value
            property_code = str(column[(column.rfind("/") + 1) :])[
                : column[(column.rfind("/") + 1) :].find(".")
            ]

            # find the property scope substring
            scope_string = column[(column.find("/") + 1) : (column.rfind("/"))]

            # find the property or SHK suffix
            p_or_shk = column[: column.find(".")].title().replace("_", "")

            rename_dict[column] = (
                property_code + "(" + scope_string + "-" + p_or_shk + ")"
            )

        response_df.rename(columns=rename_dict, inplace=True)

    if column_name_mapping:

        response_df.rename(columns=column_name_mapping, inplace=True)

    return response_df.dropna(axis=1, how="all")
