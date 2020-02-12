import pandas as pd
from flatten_json import flatten


def lusid_response_to_data_frame(lusid_response, rename_properties: bool = False):
    """
    Description:
    This function takes a LUSID API response and attempts to convert the response into a Pandas DataFrame or Series.
    Not all LUSID API responses are the same datatype. Therefore we need to implement some if/else conditional logic to
    see how the response should be converted.
    In terms of an implementation, the function checks for two attributes on the response:
    (1) A "values" attribute - these are lists of LUSID objects. Example: the "Get Holdings" response has a values
    attribute which contains a list of individual holding objects.
    (2) A "to_dict" attribute - this is one dictionary of values.
    Parameters:
    :param lusid_response: a response from the LUSID APIs (e.g. VersionedResourceListOfPortfolioHolding)
    :param bool rename_properties: this parameter formats the returned DataFrame. Specifically, the formatter does two things;
    (1) removes any metadata columns for properties and SHKs, and (2) simplifies the naming of column headers for properties and SHKs.
    Returns:
    pandas DataFrame
    """

    if hasattr(lusid_response, "values") and type(lusid_response.values) == list:

        response_df = pd.DataFrame(
            flatten(value.to_dict(), ".") for value in lusid_response.values
        )

    elif hasattr(lusid_response, "to_dict"):

        response_df = pd.DataFrame.from_dict(
            (flatten(lusid_response.to_dict(), ".")),
            orient="index",
            columns=["response_values"],
        )
    else:

        raise TypeError(
            """Cannot map response object to pandas DataFrame or Series. The LUSID response object must be
                        either values or to_dict"""
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

    return response_df.dropna(axis=1, how="all")
