import lusid
import lusid.models as models
from lusid.api import InstrumentsApi, SearchApi
import numpy as np
import time
from lusidtools.cocoon.utilities import checkargs
import pandas as pd
import logging
import re
from lusidtools.cocoon.async_tools import run_in_executor
import asyncio
from typing import Callable


@checkargs
def prepare_key(identifier_lusid: str, full_key_format: bool) -> str:
    """
    This function prepares the key for the identifier based on whether the full key or just the code is required

    Parameters
    ----------
    identifier_lusid : str
        The LUSID identifier in either full key format or code only
    full_key_format : bool
        Whether or not they key needs to be in the full key format

    Returns
    -------
    str
        The LUSID identifier in the correct format
    """

    if full_key_format:
        return (
            identifier_lusid
            if re.findall("Instrument/\S+/\S+", identifier_lusid)
            else f"Instrument/default/{identifier_lusid}"
        )
    else:
        return (
            identifier_lusid.split("/")[2]
            if re.findall("Instrument/default/\S+", identifier_lusid)
            else identifier_lusid
        )


@checkargs
def create_identifiers(
    index,
    row: pd.Series,
    file_type: str,
    instrument_identifier_mapping: dict = None,
    unique_identifiers: list = None,
    full_key_format: bool = True,
    prepare_key: Callable = prepare_key,
) -> dict:
    """

    Parameters
    ----------
    index
        The index of the row in the DataFrame
    row : pd.Series
        The row of the DataFrame to create identifiers for
    file_type : str
        The file type to create identifiers for
    instrument_identifier_mapping : dict
        The instrument identifier mapping to use
    unique_identifiers : list
        The list of allowable unique instrument identifiers
    full_key_format : bool
        Whether or not the full key format i.e. 'Instrument/default/Figi' is required
    prepare_key : callable
        The function to use to prepare the key

    Returns
    -------
    identifiers : dict
        The identifiers to use on the request
    """

    # Populate the identifiers for this instrument
    identifiers = {
        # Handles specifying the entire property key e.g. Instrument/default/Figi or just the code e.g. Figi
        prepare_key(identifier_lusid, full_key_format): models.InstrumentIdValue(
            value=row[identifier_column]
        )
        if file_type == "instrument"
        else row[identifier_column]
        for identifier_lusid, identifier_column in instrument_identifier_mapping.items()
        if not pd.isna(  # Only use the identifier it it has a value
            row[identifier_column]
        )
    }

    # If there are no identifiers raise an error
    if len(identifiers) == 0:
        raise ValueError(
            f"""The row at index {str(index)} has no value for every single one of the provided 
        identifiers. Please ensure that each row has at least one identifier and try again"""
        )

    # Check that at least one unique identifier exists if it is an instrument file (need to move this out of here)
    if file_type == "instrument":

        # Get the unique list of unique identifiers which have been populated
        unique_identifiers_populated = list(
            set(unique_identifiers).intersection(set(list(identifiers.keys())))
        )

        # If there are no unique identifiers raise an Exception as you need at least one to make a successful call
        if len(unique_identifiers_populated) == 0:
            raise ValueError(
                f"""The instrument at index {str(index)} has no value for at least one unique 
            identifier. Please ensure that each instrument has at least one unique identifier and try again. The
            allowed unique identifiers are {str(unique_identifiers)}"""
            )

    else:
        # If the transaction/holding is cash remove all other identifiers and just use this one
        if "Instrument/default/Currency" in list(identifiers.keys()):
            currency_value = identifiers["Instrument/default/Currency"]
            identifiers.clear()
            identifiers["Instrument/default/Currency"] = currency_value

    return identifiers


@checkargs
def resolve_instruments(
    api_factory: lusid.utilities.ApiClientFactory,
    data_frame: pd.DataFrame,
    identifier_mapping: dict,
):
    """
    This function attempts to resolve each row of the file to an instrument in LUSID

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        An instance of the Lusid Api Factory
    data_frame : pd.DataFrame
        The DataFrame containing the transactions or holdings to resolve to unique instruments
    identifier_mapping : dict
        The column mapping between allowable identifiers in LUSID and identifier columns in the dataframe

    Returns
    -------
    _data_frame : pd.DataFrame
        The input DataFrame with resolution columns added
    """

    if "Currency" not in list(
        identifier_mapping.keys()
    ) and "Instrument/default/Currency" not in list(identifier_mapping.keys()):
        raise KeyError(
            """There is no column specified in the identifier_mapping to identify whether or not an instrument is cash.
               Please specify add the key "Currency" or "Instrument/default/Currency" to your mapping. If no instruments
               are cash you can set the value to be None"""
        )

    if "Currency" in list(identifier_mapping.keys()):
        identifier_mapping["Instrument/default/Currency"] = identifier_mapping[
            "Currency"
        ]
        del identifier_mapping["Currency"]

    # Check that the values of the mapping exist in the DataFrame
    if not (set(identifier_mapping.values()) <= set(data_frame.columns)):
        raise KeyError(
            "there are values in identifier_mapping that are not columns in the dataframe"
        )

    # Get the allowable instrument identifiers from LUSID
    response = api_factory.build(InstrumentsApi).get_instrument_identifier_types()
    """
    # Collect the names and property keys for the identifiers and concatenate them
    allowable_identifier_names = [identifier.identifier_type for identifier in response.values]
    allowable_identifier_keys = [identifier.property_key for identifier in response.values]
    allowable_identifiers = allowable_identifier_names + allowable_identifier_keys

    # Check that the identifiers in the mapping are all allowed to be used in LUSID
    if not (set(identifier_mapping['identifier_mapping'].keys()) <= set(allowable_identifiers)):
        raise Exception(
            'there are LUSID identifiers in the identifier_mapping which are not configured in LUSID')
    """
    # Copy the data_frame to ensure the original isn't modified
    _data_frame = data_frame.copy(deep=True)

    # Set up the result Pandas Series to track resolution
    found_with = pd.Series(index=_data_frame.index, dtype=np.dtype(object))
    resolvable = pd.Series(index=_data_frame.index, dtype=np.dtype(bool))
    luid = pd.Series(index=_data_frame.index, dtype=np.dtype(object))
    comment = pd.Series(index=_data_frame.index, dtype=np.dtype(object))
    logging.info("Beginning instrument resolution process")
    # Iterate over each row in the DataFrame
    for index, row in _data_frame.iterrows():

        if index % 10 == 0:
            logging.info(f"Up to row {index}")
        # Initialise list to hold the identifiers used to resolve
        found_with_current = []
        # Initialise a value of False for the row's resolvability to an instrument in LUSID
        resolvable_current = False
        # Initilise the LUID value
        luid_current = None
        # Initialise the comment value
        comment_current = "No instruments found for the given identifiers"
        # Takes the currency resolution function and applies it
        currency = row[identifier_mapping["Instrument/default/Currency"]]

        if not pd.isna(currency):
            resolvable_current = True
            found_with_current.append(currency)
            luid_current = currency
            comment_current = "Resolved as cash with a currency"

        search_requests = [
            models.InstrumentSearchProperty(
                key=f"Instrument/default/{identifier_lusid}"
                if "Instrument/" not in identifier_lusid
                else identifier_lusid,
                value=row[identifier_dataframe],
            )
            for identifier_lusid, identifier_dataframe in identifier_mapping.items()
            if not pd.isnull(row[identifier_dataframe])
        ]

        # Call LUSID to search for instruments
        attempts = 0

        if len(search_requests) > 0:
            while attempts < 3:
                try:
                    response = api_factory.build(SearchApi).instruments_search(
                        instrument_search_property=search_requests, mastered_only=True
                    )
                    break
                except lusid.exceptions.ApiException as error_message:
                    attempts += 1
                    comment_current = f"Failed to find instrument due to LUSID error during search due to status {error_message.status} with reason {error_message.reason}"
                    time.sleep(5)

            if attempts == 3:
                # Update the luid series
                luid.iloc[index] = luid_current
                # Update the found with series
                found_with.iloc[index] = found_with_current
                # Update the resolvable series
                resolvable.iloc[index] = resolvable_current
                # Update the comment series
                comment.iloc[index] = comment_current
                continue

            search_request_number = -1

            for result in response:

                search_request_number += 1
                # If there are matches
                if len(result.mastered_instruments) == 1:
                    # Add the identifier responsible for the successful search request to the list
                    found_with_current.append(
                        search_requests[search_request_number].key.split("/")[2]
                    )
                    comment_current = (
                        "Uniquely resolved to an instrument in the securities master"
                    )
                    resolvable_current = True
                    luid_current = (
                        result.mastered_instruments[0]
                        .identifiers["LusidInstrumentId"]
                        .value
                    )
                    break

                elif len(result.mastered_instruments) > 1:
                    comment_current = f'Multiple instruments found for the instrument using identifier {search_requests[search_request_number].key.split("/")[2]}'
                    resolvable_current = False
                    luid_current = np.NaN

        # Update the luid series
        luid.iloc[index] = luid_current
        # Update the found with series
        found_with.iloc[index] = found_with_current
        # Update the resolvable series
        resolvable.iloc[index] = resolvable_current
        # Update the comment series
        comment.iloc[index] = comment_current

    # Add the series to the dataframe
    _data_frame["resolvable"] = resolvable
    _data_frame["foundWith"] = found_with
    _data_frame["LusidInstrumentId"] = luid
    _data_frame["comment"] = comment

    return _data_frame


@checkargs
def get_unique_identifiers(api_factory: lusid.utilities.ApiClientFactory):

    """
    Tests getting the unique instrument identifiers

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The LUSID api factory to use

    Returns
    -------
    list[str]
        The property keys of the available identifiers
    """
    # Get the allowed instrument identifiers from LUSID
    identifiers = api_factory.build(
        lusid.api.InstrumentsApi
    ).get_instrument_identifier_types()

    # Return the identifiers that are configured to be unique
    return [
        identifier.identifier_type
        for identifier in identifiers.values
        if identifier.is_unique_identifier_type
    ]


async def enrich_instruments(
    api_factory: lusid.utilities.ApiClientFactory,
    data_frame: pd.DataFrame,
    instrument_identifier_mapping: dict,
    mapping_required: dict,
    constant_prefix: str = "$",
    **kwargs,
):
    search_requests_all = []

    for index, row in data_frame.iterrows():
        search_requests_instrument = [
            lusid.models.InstrumentSearchProperty(
                key=identifier_lusid
                if re.findall("Instrument/default/\S+", identifier_lusid)
                else f"Instrument/default/{identifier_lusid}",
                value=row[identifier_column],
            )
            for identifier_lusid, identifier_column in instrument_identifier_mapping.items()
            if not pd.isna(row[identifier_column])
        ]

        search_requests_all.append(search_requests_instrument)

    responses = await asyncio.gather(
        *[
            instrument_search(
                api_factory=api_factory, search_requests=search_requests, **kwargs
            )
            for search_requests in search_requests_all
        ],
        return_exceptions=False,
    )

    names = []

    for response in responses:
        name = np.NaN
        for single_search in response:
            if isinstance(single_search, Exception):
                logging.warning(single_search)
                continue
            elif len(single_search[0].external_instruments) > 0:
                name = single_search[0].external_instruments[0].name
                break
        names.append(name)

    enriched_column_name = "LUSID.Name.Enriched"

    data_frame[enriched_column_name] = names

    # Missing mapping for name altogether
    if "name" not in list(mapping_required.keys()):
        mapping_required["name"] = enriched_column_name

    # A column for name already exists and needs to be enriched
    elif (
        isinstance(mapping_required["name"], str)
        and mapping_required["name"][0] != constant_prefix
    ):
        data_frame[mapping_required["name"]] = data_frame[
            mapping_required["name"]
        ].fillna(value=data_frame[enriched_column_name])

    elif isinstance(mapping_required["name"], dict) and "column" in list(
        mapping_required["name"].keys()
    ):
        data_frame[mapping_required["name"]["column"]] = data_frame[
            mapping_required["name"]["column"]
        ].fillna(value=data_frame[enriched_column_name])

    # Is a constant specified by the constant prefix
    elif (
        isinstance(mapping_required["name"], str)
        and mapping_required["name"][0] == constant_prefix
    ):
        mapping_required["name"] = {"default": mapping_required["name"][1:]}
        mapping_required["name"]["column"] = enriched_column_name

    # Is a constant specified by the default nested dictionary specification
    elif (
        isinstance(mapping_required["name"], dict)
        and "default" in list(mapping_required["name"].keys())
        and "column" not in list(mapping_required["name"].keys())
    ):
        mapping_required["name"]["column"] = enriched_column_name

    return data_frame, mapping_required


async def instrument_search(
    api_factory: lusid.utilities.ApiClientFactory, search_requests: list, **kwargs
) -> list:
    """
    Conducts an instrument search for a single instrument

    Parameters
    ----------
    api_factory :       lusid.utilities.ApiClientFactory
                        The api factory to use
    search_requests:    list[lusid.models.InstrumentSearchProperty]
                        The search requests for this instrument
    kwargs

    Returns
    -------
    None

    """

    instrument_search_results = []

    for search_request in search_requests:
        try:
            result = await instrument_search_single(
                api_factory, search_request, **kwargs
            )
            instrument_search_results.append(result)
        except lusid.exceptions.ApiException as e:
            instrument_search_results.append(e)
    return instrument_search_results


@run_in_executor
def instrument_search_single(
    api_factory: lusid.utilities.ApiClientFactory,
    search_request: lusid.models.InstrumentSearchProperty,
    **kwargs,
) -> list:
    """
    Conducts an instrument search with a single search request

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The Api factory to use
    search_request : lusid.models.InstrumentSearchProperty
        The search request
    kwargs

    Returns
    -------
    list[lusid.models.InstrumentMatch]
        The results of the search
    """

    return lusid.api.SearchApi(
        api_factory.build(lusid.api.SearchApi)
    ).instruments_search(instrument_search_property=[search_request])
