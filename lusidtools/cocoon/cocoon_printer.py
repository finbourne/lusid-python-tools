from typing import Tuple

import pandas as pd
import numpy as np
from lusidtools.cocoon import _checkargs
from lusid.exceptions import ApiException


@_checkargs
def _check_dict_for_required_keys(
        target_dict: dict, target_name: str, required_keys: list
):
    """
    Checks that a named dictionary contains a list of required keys

    If the key does not exist then a ValueError is thrown

    Parameters
    ----------
    target_dict:    dict
                    Dictionary to check for keys in
    target_name:    str
                    Variable name of dictionary
    required_keys:  list
                    List of required keys

    Returns
    -------

    """
    for key in required_keys:
        if key not in target_dict.keys():
            raise ValueError(
                f"'{key}' missing from {target_name}. {target_name} must include {required_keys}"
                f"as keys"
            )


def _get_errors_from_response(list_of_API_exceptions: list):
    """

    Parameters
    ----------
    list_of_API_exceptions: list
                            A list of LUSID APIexceptions containing infomation on why the request failed

    Returns
    -------
    errors: pd.DataFrame
            a DataFrame containing the reasons and status codes for ApiExceptions from a request
    """
    # check all items are ApiExceptions
    for i in list_of_API_exceptions:
        if not isinstance(i, ApiException):
            raise TypeError(
                f" Unexpected Error in response. Expected instance of ApiException, but got: {repr(i)}"
            )

    # get status and reason for each batch
    status = [item.status for item in list_of_API_exceptions]
    reason = [item.reason for item in list_of_API_exceptions]

    # transpose list of lists for insertion to dataframe
    items_error = np.array([reason, status]).T.tolist()

    return pd.DataFrame(items_error, columns=["error_items", "status"])


def _get_portfolio_from_href(href: list, file_type: str):
    """
    gets portfolio codes from hrefs

    The portfolio code is the second last item in the href URL

    Parameters
    ----------
    href:       list
                Full href from LUSID response
    file_type:  str
                type of data to be upserted

    Returns
    -------
    codes:      list
                portfolio codes taken from hrefs

    """

    # get portfolio codes from each href
    codes = [j[-2] for j in [i.split("/") for i in href]]

    return codes


@_checkargs
def _get_non_href_response(response: dict, file_type: str):
    items_success = [
        key for batch in response[file_type]["success"] for key in batch.values.keys()
    ]

    items_failed = [
        key for batch in response[file_type]["success"] for key in batch.failed.keys()
    ]
    return items_success, items_failed


def format_instruments_response(
        response: dict,
)-> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Unpacks data from instruments response

    This function unpacks a response from instrument requests and returns successful, failed and errored statuses for
    request constituents.

    Parameters
    ----------
    response:   dict
                response from Lusid-python-tools

    Returns
    -------
    success:    pandas.DataFrame
                Successful calls from request
    errors:     pandas.DataFrame
                Error responses from request that fail (APIExceptions: 400 errors)
    failed:     pandas.DataFrame
                Failed responses that LUSID rejected

    """

    file_type = "instruments"
    _check_dict_for_required_keys(
        response[file_type], f"Response from {file_type} request", ["errors", "success"]
    )

    # get success and failures
    items_success, items_failed = _get_non_href_response(response, file_type)

    items = (pd.DataFrame(items_success, columns=["successful items"]), _get_errors_from_response(
        response[file_type]["errors"]), pd.DataFrame(items_failed, columns=["failed_items"]))
    return items


def format_portfolios_response(response: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Unpacks data from portfolio response

    This function unpacks a response from portfolio requests and returns successful and errored statuses for
    request constituents.

    Parameters
    ----------
    response:   dict
                response from Lusid-python-tools

    Returns
    -------
    success:    pd.DataFrame
                Successful calls from request
    errors:     pd.DataFrame
                Error responses from request that fail (APIExceptions: 400 errors)
    """

    file_type = "portfolios"
    _check_dict_for_required_keys(
        response[file_type], f"Response from {file_type} request", ["errors", "success"]
    )

    # get success
    items_success = [batch.id.code for batch in response[file_type]["success"]]

    errors = _get_errors_from_response(response[file_type]["errors"])

    return (pd.DataFrame(items_success, columns=["successful items"]), errors)


def format_transactions_response(response: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Unpacks data from transactions response

    This function unpacks a response from transaction requests and returns successful and errored statuses for
    request constituents.

    Parameters
    ----------
    response:   dict
                response from Lusid-python-tools

    Returns
    -------
    success:    pd.DataFrame
                Successful calls from request

    errors:     pd.DataFrame
                 Error responses from request that fail (APIExceptions: 400 errors)

    """

    file_type = "transactions"

    _check_dict_for_required_keys(
        response[file_type], f"Response from {file_type} request", ["errors", "success"]
    )

    # get success
    items_success = [batch.href for batch in response[file_type]["success"]]

    errors = _get_errors_from_response(response[file_type]["errors"])

    return (
        pd.DataFrame(
            _get_portfolio_from_href(items_success, file_type),
            columns=["successful items"],
        ),
        errors,
    )


def format_holdings_response(response: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Unpacks data from holdings response

    This function unpacks a response from holding requests and returns successful and errored statuses for
    request constituents.

    Parameters
    ----------
    response:   dict
                holdings response containing information on status of upsert request

    Returns
    -------
    success:    pd.DataFrame
                Successful calls from request
    error:      pd.DataFrame
                Error responses from request that fail (APIExceptions: 400 errors)
    """

    file_type = "holdings"
    _check_dict_for_required_keys(
        response[file_type], f"Response from {file_type} request", ["errors", "success"]
    )

    # get success
    items_success = [batch.href for batch in response[file_type]["success"]]

    errors = _get_errors_from_response(response[file_type]["errors"])

    return (
        pd.DataFrame(
            _get_portfolio_from_href(items_success, file_type),
            columns=["successful items"],
        ),
        errors,
    )


def format_quotes_response(
        response: dict,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Unpacks data from quotes response

    This function unpacks a response from quotes requests and returns successful, failed and errored statuses for
    request constituents.

    Parameters
    ----------
    response:   dict
                response from Lusid-python-tools

    Returns
    -------
    success:    pd.DataFrame
                Successful calls from request
    error:      pd.DataFrame
                Error responses from request that fail (APIExceptions: 400 errors)
    failed:     pd.DataFrame
                Failed responses that LUSID rejected

    """
    """
    This function unpacks a response from quotes requests and returns successful, failed and errored statuses for
    request constituents.

    :param dict response: response from Lusid-python-tools
    :return: pd.DataFrame: Successful calls from request
    :return: pd.DataFrame: Error responses from request that fail (APIExceptions: 400 errors)
    :return: pd.DataFrame: Failed responses that LUSID rejected
    """
    file_type = "quotes"

    _check_dict_for_required_keys(
        response[file_type], f"Response from {file_type} request", ["errors", "success"]
    )
    items_success, items_failed = _get_non_href_response(response, file_type)

    return (
        pd.DataFrame(items_success, columns=["successful items"]),
        _get_errors_from_response(response[file_type]["errors"]),
        pd.DataFrame(items_failed, columns=["failed_items"]),
    )
