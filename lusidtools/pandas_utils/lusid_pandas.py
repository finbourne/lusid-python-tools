import pandas as pd
from flatten_json import flatten


def lusid_response_to_data_frame(lusid_response):

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

    return response_df
