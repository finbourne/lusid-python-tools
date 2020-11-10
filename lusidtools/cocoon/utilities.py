import argparse
import copy
import csv
import os
import uuid

import numpy as np
import lusid
from collections.abc import Mapping
import pandas as pd
from detect_delimiter import detect
import requests
import json
import inspect
import functools
from pathlib import Path
import re
from lusidtools.cocoon.dateorcutlabel import DateOrCutLabel
import lusid.models as models
import logging
import time as default_time
from lusidtools.cocoon.validator import Validator
import types
import typing


def checkargs(function: typing.Callable) -> typing.Callable:
    """
    This can be used as a decorator to test the type of arguments are correct. It checks that the provided arguments
    match any type annotations and/or the default value for the parameter.

    Parameters
    ----------
    function : typing.Callable
        The function to wrap with annotated types, all parameters must be annotated with a type

    Returns
    -------
    _f : typing.Callable
        The wrapped function
    """

    @functools.wraps(function)
    def _f(*args, **kwargs):

        # Get all the function arguments in order
        function_arguments = inspect.signature(function).parameters

        # Collect each non keyword argument value and key it by the argument name
        keyed_arguments = {
            list(function_arguments.keys())[i]: args[i] for i in range(0, len(args))
        }

        # Update this with the keyword argument values
        keyed_arguments.update(kwargs)

        # For each argument raise an error if it is of the incorrect type and if it has an invalid default value
        for argument_name, argument_value in keyed_arguments.items():

            if argument_name not in list(function_arguments.keys()):
                raise ValueError(
                    f"The argument {argument_name} is not a valid keyword argument for this function, valid arguments"
                    + f" are {str(list(function_arguments.keys()))}"
                )

            # Get the arguments details
            argument_details = function_arguments[argument_name]
            # Assume that there is no default value for this parameter
            is_default_value = False

            # If there is a default value
            if argument_details.default is not argument_details.empty:
                # Check to see if the argument value matches the default
                if argument_details.default is None:
                    is_default_value = argument_value is argument_details.default
                else:
                    is_default_value = argument_value == argument_details.default

            # If the argument value is of the wrong type e.g. list instead of dict then throw an error
            if (
                not isinstance(argument_value, argument_details.annotation)
                and argument_details.annotation is not argument_details.empty
            ):
                # Only exception to this is if it matches the default value which may be of a different type e.g. None
                if not is_default_value:
                    raise TypeError(
                        f"""The value provided for {argument_name} is of type {type(argument_value)} not of 
                    type {argument_details.annotation}. Please update the provided value to be of type 
                    {argument_details.annotation}"""
                    )

        return function(*args, **kwargs)

    return _f


def make_code_lusid_friendly(raw_code) -> str:
    """
    This function takes a column name and converts it to a LUSID friendly code creating LUSID objects. LUSID allows
    for up to 64 characters which can be lowercase and uppercase letters, numbers, a dash ("-") or an underscore ("_").
    The complete restrictions are here: https://support.lusid.com/what-is-a-code

    Parameters
    ----------
    raw_code : any
        A raw column header which needs special characters stripped out

    Returns
    -------
    friendly_code : str
        A LUSID friendly code with special characters removed
    """

    # Convert any type to a string
    try:
        raw_code = str(raw_code)
    except Exception as exception:
        raise ValueError(
            f"Could not convert value of {raw_code} with type {type(raw_code)} to a string. "
            + "Please convert to a format which can be cast to a string and try again"
        ) from exception

    # Check that it does not exceed the max length
    max_length = 64

    if len(raw_code) > max_length:
        raise ValueError(
            f"""The name {raw_code} is {len(raw_code)} characters long and exceeds the limit of {max_length}
                             for a code. Please shorten it by {len(raw_code) - 64} characters."""
        )

    # Specifically convert known unfriendly characters with a specific string and remove the rest completely
    friendly_code = re.sub(
        r"[^-\w]",
        "",
        raw_code.replace("%", "Percentage")
        .replace("&", "and")
        .replace(".", "_")
        .strip(),
    )

    return friendly_code


@checkargs
def populate_model(
    model_object_name: str,
    required_mapping: dict,
    optional_mapping: dict,
    row: pd.Series,
    properties,
    identifiers: dict = None,
    sub_holding_keys=None,
) -> typing.Callable:
    """
    This function populates the provided LUSID model object in lusid.models with values from a Pandas Series

    Parameters
    ----------
    model_object_name : str
        The name of the model object to populate
    required_mapping : dict
        The required mapping between the row columns and the model attributes
    optional_mapping : dict
        The optional mapping between the row columns and the model attributes
    row : pd.Series
        The row from the provided pd.DataFrame to use to populate the model
    properties
        The properties for this model
    identifiers : dict
        The identifiers for this model
    sub_holding_keys
        The sub holding keys to use

    Returns
    -------
    set_attributes : typing.Callable
        The function to set the attributes for the model
    """

    # Check that the provided model name actually exists
    model_object = getattr(lusid.models, model_object_name, None)

    if model_object is None:
        raise TypeError("The provided model_object is not a lusid.model object")

    # Expand the mapping out from being a dot separated flat dictionary e.g. transaction_price.price to being nested
    update_dict(required_mapping, optional_mapping)

    mapping_expanded = expand_dictionary(required_mapping)

    # Set the attributes on the model
    return set_attributes_recursive(
        model_object=model_object,
        mapping=mapping_expanded,
        row=row,
        properties=properties,
        identifiers=identifiers,
        sub_holding_keys=sub_holding_keys,
    )


@checkargs
def set_attributes_recursive(
    model_object,
    mapping: dict,
    row: pd.Series,
    properties=None,
    identifiers: dict = None,
    sub_holding_keys=None,
):
    """
    This function takes a lusid.model object name and an expanded mapping between its attributes and the provided
    row of data and constructs a populated model

    Parameters
    ----------
    model_object : lusid.models
        The object from lusid.models to populate
    mapping : dict
        The expanded dictionary mapping the Series columns to the LUSID model attributes
    row : pd.Series
        The current row of the DataFrame being worked on
    properties : any
        The properties to use on this model
    identifiers : any
        The instrument identifiers to use on this model
    sub_holding_keys
        The sub holding keys to use on this model

    Returns
    -------
    new model_object : lusid.models
        An instance of the model object with populated attributes
    """

    # Get the object attributes
    obj_attr = model_object.openapi_types
    obj_init_values = {}

    # Additional attributes which are used on most models but will be populated outside the provided mapping
    additional_attributes = {
        "instrument_identifiers": identifiers,
        "properties": properties,
        "sub_holding_keys": sub_holding_keys,
        "identifiers": identifiers,
    }

    # Generate the intersection between the available attributes and the provided attributes
    provided_attributes = set(list(mapping.keys()) + list(additional_attributes.keys()))
    available_attributes = set(list(obj_attr.keys()))
    populate_attributes = provided_attributes.intersection(available_attributes)

    # Used to check if all attributes are none
    total_count = 0
    none_count = 0

    # For each of the attributes to populate
    for key in list(populate_attributes):

        # If it is an additional attribute, populate it with the provided values and move to the next attribute
        if key in list(additional_attributes.keys()):
            obj_init_values[key] = additional_attributes[key]
            continue

        # This block keeps track of the number of missing (non-additional) attributes
        else:
            total_count += 1
            if mapping[key] is None:
                none_count += 1

        # Get the attribute type
        attribute_type = obj_attr[key]

        # If this is the last object and there is no more nesting set the value from the row
        if not isinstance(mapping[key], dict):
            # If this exists in the mapping with a value and there is a value in the row for it
            if mapping[key] is not None and not pd.isna(row[mapping[key]]):
                # Converts to a date if it is a date field
                if "date" in key or "created" in key or "effective_at" in key:
                    obj_init_values[key] = str(DateOrCutLabel(row[mapping[key]]))
                else:
                    obj_init_values[key] = row[mapping[key]]

        # if there is more nesting call the function recursively
        else:
            # Ensure that that if there is a complex attribute type e.g. dict(str, InstrumentIdValue) it is extracted
            attribute_type, nested_type = extract_lusid_model_from_attribute_type(
                attribute_type
            )

            # Call the function recursively
            value = set_attributes_recursive(
                model_object=getattr(lusid.models, attribute_type),
                mapping=mapping[key],
                row=row,
            )

            obj_init_values[key] = [value] if nested_type == "list" else value

    """
    If all attributes are None propagate None rather than a model filled with Nones. For example if a CorporateActionSourceId
    has no scope or code return build a model with CorporateActionSourceId = None rather than CorporateActionSourceId = 
    lusid.models.ResourceId(scope=None, code=None)
    """
    if total_count == none_count:
        return None

    # Create an instance of and populate the model object
    return model_object(**obj_init_values)


@checkargs
def update_dict(orig_dict: dict, new_dict) -> None:
    """
    This is used to update a dictionary with another dictionary. Using the default Python update method does not merge
    nested dictionaries. This method allows for this. This modifies the original dictionary in place.

    Parameters
    ----------
    orig_dict : dict
        The original dictionary to update
    new_dict : dict
        The new dictionary to merge with the original

    Returns
    -------
    orig_dict : dict
        The updated original dictionary
    """

    # Iterate over key value pairs in the new dictionary to merge into the original
    for key, val in new_dict.items():
        # If a mapping object (e.g. dictionary) call the function recursively
        if isinstance(val, Mapping):
            tmp = update_dict(orig_dict.get(key, {}), val)
            orig_dict[key] = tmp
        # If a list then merge it into the original dictionary
        elif isinstance(val, list):
            orig_dict[key] = orig_dict.get(key, []) + val
        # Do the same for any other type
        else:
            orig_dict[key] = new_dict[key]

    return orig_dict


@checkargs
def expand_dictionary(dictionary: dict, key_separator: str = ".") -> dict:
    """
    Takes a flat dictionary (no nesting) with keys separated by a separator and converts it into a nested
    dictionary

    Parameters
    ----------
    dictionary : dict
        The input dictionary with separated keys
    key_separator : str
        The seprator to use

    Returns
    -------
    dict_expanded : dict
        The expanded nested dictionary
    """

    dict_expanded = {}

    # Loop over each composite key and final value
    for key, value in dictionary.items():
        # Split the key on the separator
        components = key.split(key_separator)
        # Get the expanded dictionary for this key and update the master dictionary
        update_dict(
            dict_expanded, expand_dictionary_single_recursive(0, components, value)
        )

    return dict_expanded


@checkargs
def expand_dictionary_single_recursive(index: int, key_list: list, value) -> dict:
    """
    Takes a list of keys and a value and turns it into a nested dictionary. This is a recursive function.

    Parameters
    ----------
    index : int
        The current index of the key in the list of keys
    key_list : list[str]
        The list of keys to turn into a nested dictionary
    value : any
        The final value to match against the last (deepest) key

    Returns
    -------
    dict
        The final value to match against the last (deepest) key
    """

    # Gets the current key in the list
    key = key_list[index]

    # If it is the last key in the list return a dictionary with it keyed against the value
    if key == key_list[-1]:
        return {key: value}

    # Otherwise if it is not the last key, key it against calling this function recursively with the next key
    return {key: expand_dictionary_single_recursive(index + 1, key_list, value)}


@checkargs
def get_swagger_dict(api_url: str) -> dict:
    """
    Gets the lusid.json swagger file

    Parameters
    ----------
    api_url : str
        The base api url for the LUSID instance

    Returns
    -------
    dict
        The swagger file as a dictionary
    """

    swagger_path = "/swagger/v0/swagger.json"
    swagger_url = api_url + swagger_path
    swagger_file = requests.get(swagger_url)

    if swagger_file.status_code == 200:
        swagger = json.loads(swagger_file.text)

        app_name = swagger.get("info", {}).get("title", {})
        if app_name is None or app_name != "LUSID API":
            raise ValueError(f"Invalid LUSID OpenAPI file: {swagger_url}")

        return swagger
    else:
        raise ValueError(
            f"""Received a {swagger_file.status_code} response from the provided url, please double check
                             the base api url and try again"""
        )


def generate_required_attributes_list():
    pass


@checkargs
def verify_all_required_attributes_mapped(
    mapping: dict,
    model_object_name: str,
    exempt_attributes: list = None,
    key_separator: str = ".",
) -> None:
    """
    Verifies that all required attributes are included in the mapping, passes silently if they are and raises an exception
    otherwise

    Parameters
    ----------
    mapping : dict
        The required mapping
    model_object_name : str
        The name of the lusid.models object that the mapping is for
    exempt_attributes : list[str]
        The attributes that are exempt from needing to be in the required mapping
    key_separator : str
        The separator to use to join the required attributes together

    Returns
    -------
    key_separator : str
        The separator to use to join the required attributes together
    """

    # Check that the provided model name actually exists
    model_object = getattr(lusid.models, model_object_name, None)

    if model_object is None:
        raise TypeError("The provided model_object is not a lusid.model object")

    # Convert a None to an empty list
    exempt_attributes = (
        Validator(exempt_attributes, "exempt_attributes")
        .set_default_value_if_none([])
        .value
    )

    # Gets the required attributes for this model
    required_attributes = get_required_attributes_model_recursive(
        model_object=model_object, key_separator=key_separator
    )

    # Removes the exempt attributes
    for attribute in required_attributes:
        # Removes all nested attributes for example if "identifiers" is exempt "identifiers.value" will be removed
        if attribute.split(key_separator)[0] in exempt_attributes:
            required_attributes.remove(attribute)

    missing_attributes = set(required_attributes) - set(list(mapping.keys()))

    if len(missing_attributes) > 0:
        raise ValueError(
            f"""The required attributes {str(missing_attributes)} are missing from the mapping. Please
                             add them."""
        )


@checkargs
def get_required_attributes_model_recursive(model_object, key_separator: str = "."):
    """
    This is a recursive function which gets all of the required attributes on a LUSID model. If the model is nested
    then it separates the attributes by a '.' until the bottom level where no more models are required and a primitive
    type is supplied e.g. string, int etc.

    Parameters
    ----------
    model_object : lusid.model
        The model to get required attributes for
    key_separator : str
        The separator to use to join the required attributes together

    Returns
    -------
    list[str]
        The required attributes of the model
    """

    attributes = []

    # Get the required attributes for the current model
    required_attributes = get_required_attributes_from_model(model_object)

    # Get the types of the attributes for the current model
    open_api_types = model_object.openapi_types

    for required_attribute in required_attributes:

        required_attribute_type = open_api_types[required_attribute]

        # Check to see if there is a LUSID model for this required attribute, if no further nesting then add this attribute
        if not check_nested_model(required_attribute_type):
            attributes.append(camel_case_to_pep_8(required_attribute))

        # Otherwise call the function recursively
        else:
            # Ensure that that if there is a complex attribute type e.g. dict(str, InstrumentIdValue) it is extracted
            (
                required_attribute_type,
                nested_type,
            ) = extract_lusid_model_from_attribute_type(required_attribute_type)

            nested_required_attributes = get_required_attributes_model_recursive(
                model_object=getattr(lusid.models, required_attribute_type),
            )

            for nested_required_attribute in nested_required_attributes:
                attributes.append(
                    key_separator.join(
                        [
                            camel_case_to_pep_8(required_attribute),
                            nested_required_attribute,
                        ]
                    )
                )

    return attributes


def get_required_attributes_from_model(model_object) -> list:
    """
    Gets the required attributes for a LUSID model using reflection

    Parameters
    ----------
    model_object : lusid.models
        A LUSID model object

    Returns
    -------
    list[str]
        The required attributes
    """

    # Get the source code for the model
    model_details = inspect.getsource(model_object)

    # Get all the setter function definitions
    setters = re.findall(r"(?<=.setter).+?(?:@|to_dict)", model_details, re.DOTALL)

    # Set the status (required or optional) for each attribute based on whether "is None:" exists in the setter function
    '''
    Here are two examples
    
    A) A None value is not allowed and hence this is required. Notice the "if identifiers is None:" condition. 
    
    @identifiers.setter
    def identifiers(self, identifiers):
        """Sets the identifiers of this InstrumentDefinition.
        A set of identifiers that can be used to identify the instrument. At least one of these must be configured to be a unique identifier.  # noqa: E501
        :param identifiers: The identifiers of this InstrumentDefinition.  # noqa: E501
        :type: dict(str, InstrumentIdValue)
        """
        if identifiers is None:
            raise ValueError("Invalid value for `identifiers`, must not be `None`")  # noqa: E501

        self._identifiers = identifiers
        
    B) A None value is allowed and hence this is optional
    
    @look_through_portfolio_id.setter
    def look_through_portfolio_id(self, look_through_portfolio_id):
        """Sets the look_through_portfolio_id of this InstrumentDefinition.
        :param look_through_portfolio_id: The look_through_portfolio_id of this InstrumentDefinition.  # noqa: E501
        :type: ResourceId
        """

        self._look_through_portfolio_id = look_through_portfolio_id

    '''
    attribute_status = {
        re.search(r"(?<=def ).+?(?=\(self)", setter).group(0): "Required"
        if "is None:" in setter
        else "Optional"
        for setter in setters
    }

    # If there are required attributes collect them as a list
    required_attributes = [
        key for key, value in attribute_status.items() if value == "Required"
    ]

    # If there are no required attributes on a model, assume that all attributes are required
    # This is for cases such as lusid.models.TransactionRequest.transaction_price
    if len(required_attributes) == 0:
        required_attributes = list(attribute_status.keys())

    return required_attributes


def extract_lusid_model_from_attribute_type(attribute_type: str) -> str:
    """
    Extracts a LUSID model from a complex attribute type e.g. dict(str, InstrumentIdValue) if it exists. If there
    is no LUSID model the attribute type is still returned

    Parameters
    ----------
    attribute_type : str
        The attribute type to extract the model from

    Returns
    -------
    attribute_type : str
        The returned attribute type with the LUSID model extracted if possible
    nested_type : str
        The type of nesting used e.g. List or Dict
    """

    nested_type = None

    # If the attribute type is a dictionary e.g. dict(str, InstrumentIdValue), extract the type
    if "dict" in attribute_type:
        attribute_type = attribute_type.split(", ")[1].rstrip(")")
        nested_type = "dict"
    # If it is a list e.g. list[ModelProperty] extract the type
    if "list" in attribute_type:
        attribute_type = attribute_type.split("list[")[1].rstrip("]")
        nested_type = "list"

    return attribute_type, nested_type


@checkargs
def check_nested_model(required_attribute_type: str) -> bool:
    """
    Takes the properties of a required attribute on a model and searches as to whether or not this attribute
    requires a model of its own

    Parameters
    ----------
    required_attribute_type : str
        The type of the required attribute

    Returns
    -------
    str
        The name of the LUSID model
    """

    required_attribute_type, nested_type = extract_lusid_model_from_attribute_type(
        required_attribute_type
    )

    top_level_model = getattr(lusid.models, required_attribute_type, None)

    if top_level_model is None:
        return False

    return True


@checkargs
def gen_dict_extract(key, var: dict):
    """
    Searches a nested dictionary for a key, yielding any values it finds against that key

    Parameters
    ----------
    key : str
        The key to search for
    var : dict
        The dictionary to search

    Returns
    -------
    generator(result)
        A generator with the results
    """

    if hasattr(var, "items"):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(key, d):
                        yield result


@checkargs
def camel_case_to_pep_8(attribute_name: str) -> str:
    """
    Converts a camel case name to PEP 8 standard

    Parameters
    ----------
    attribute_name : str
        The camel case attribute name

    Returns
    -------
    str
        The PEP 8 formatted attribute name
    """

    matches = re.finditer(
        ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", attribute_name
    )
    return "_".join([m.group(0)[0].lower() + m.group(0)[1:] for m in matches])


def convert_cell_value_to_string(data):
    """
    Converts the value of a call to a string if it is a list or a dictionary

    Parameters
    ----------
    data
        The value of the cell in the dataframe

    Returns
    -------
    str
        The original data if it is not a list or a dictionary, otherwise the string representation of these

    """

    if isinstance(data, list):
        return ", ".join(data)

    elif isinstance(data, dict):
        return str(data)

    else:
        return data


def handle_nested_default_and_column_mapping(
    data_frame: pd.DataFrame, mapping: dict, constant_prefix: str = "$"
):
    """
    This function handles when a mapping is provided which contains as a value a dictionary with a column and/or default
    key rather than just a string with the column name. It populates the DataFrame with the default value as appropriate
    and removes the nesting so that the model can be populated later.

    Parameters
    ----------
    data_frame : pd.DataFrame
        The updated dataframe
    mapping : dict
        The original mapping (can be required or optional)
    constant_prefix : str
        The prefix that can be used to specify a constant

    Returns
    -------
    dataframe : pd.DataFrame
        The updated DataFrame
    mapping_updated : dict
        The updated mapping
    """

    mapping_updated = {}

    for key, value in mapping.items():

        # If the value of the mapping is a dictionary
        if isinstance(value, dict):

            # If the dictionary contains a column and a default, fill nulls with the default in that column
            if ("column" in list(value.keys())) and ("default" in list(value.keys())):
                mapping_updated[key] = value["column"]
                data_frame[mapping_updated[key]] = data_frame[
                    mapping_updated[key]
                ].fillna(value["default"])

            # If there is only a default specified, create a new column filled with the default
            elif not ("column" in list(value.keys())) and (
                "default" in list(value.keys())
            ):
                mapping_updated[key] = f"LUSID.{key}"
                data_frame[mapping_updated[key]] = value["default"]

            # If there is only a column specified unnest it
            elif ("column" in list(value.keys())) and not (
                "default" in list(value.keys())
            ):
                mapping_updated[key] = value["column"]

            else:
                raise KeyError(
                    f"""You have passed in a dictionary as the value for the mapping for {key}, however 
                                   it does not contain a key for "column" or "default". Please provide a key, value
                                   pair for one or both of these keys with the column being the column name and default
                                   value being the default value to use. Alternatively just provide a string which
                                   is the column name to use."""
                )

        elif isinstance(value, str):

            if len(value) == 0:
                raise IndexError(
                    f"Unspecified mapping field: {key}. Please assign a value or remove this from the "
                    f"mapping"
                )

            if value[0] != constant_prefix:
                mapping_updated[key] = value
            else:
                mapping_updated[key] = f"LUSID.{key}"
                data_frame[mapping_updated[key]] = value[1:]

        elif isinstance(value, int):
            mapping_updated[key] = f"LUSID.{key}"
            data_frame[mapping_updated[key]] = value

        else:
            raise ValueError(
                f"""You have passed in a value with type {type(value)} for the mapping for {key}, this is
                                 not a supported type. Please provide a string with the column name to use, a constant
                                 value prefixed by {constant_prefix},an integer value or a dictionary
                                 with the keys "column" and "default" where column is the column name and default
                                 being the default value to use."""
            )

    return data_frame, mapping_updated


def load_json_file(file_path: str) -> dict:
    """

    Parameters
    ----------
    file_path : str
        relative_file_path

    Returns
    -------
    data : dict
        parsed data from json file
    """

    if not os.path.isabs(file_path):
        file_path = Path(__file__).parent.joinpath(file_path)
    if not os.path.exists(file_path):
        raise OSError(f"Json file not found at {file_path}")
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data


@checkargs
def load_data_to_df_and_detect_delimiter(args: dict) -> pd.DataFrame:
    """
    This function loads data from given file path and converts it into a pandas DataFrame

    Parameters
    ----------
    args : dict
        Arguments parsed in from command line, containing args["file_path"]

    Returns
    -------
    pd.DataFrame : pd.dataframe
        DataFrame containing data
    """
    if not os.path.exists(args["file_path"]):
        raise OSError(f"file path {args['file_path']} does not exist")

    with open(args["file_path"], "r") as read_file:
        logging.info(f"loading data from {args['file_path']}")
        data = csv.reader(read_file, lineterminator=args["line_terminator"])

        # iterate over data in unrelated lines to get to the first line of data that we are interested in
        for pre_amble in range(args["num_header"]):
            read_file.readline()

        # now that we are at the first line of data, get the header row, that will contain the formatting we are
        # interested in
        header_line = read_file.readline()

        if not args["delimiter"]:
            args["delimiter"] = get_delimiter(header_line)

            if args["delimiter"] == header_line:
                err = (
                    f"Unable to detect delimiter from first line of data at line at line number: {args['num_header']}: "
                    f"\n\t>> "
                    f"{header_line}"
                )
                raise ValueError(err)

    with open(args["file_path"], "r") as read_file:
        # read data from lines specified at command line by num_header and num_footer
        return pd.read_csv(
            args["file_path"],
            delimiter=args["delimiter"],
            header=args["num_header"],
            skipfooter=args["num_footer"],
            engine="python",
        )


def get_delimiter(sample_string: str):
    return detect(sample_string).replace("\\", "\\\\")


def check_mapping_fields_exist(
    required_list: list, search_list: list, file_type: str
) -> list:
    """
    This function checks that items in one list exist in another list

    Parameters
    ----------
    required_list : list[str]
        list of items to search for
    search_list : list[str]
        list to search in
    file_type : list[str]
        the file type of the data eg.instruments, holdings, transactions

    Returns
    -------
    missing_fields : list[str]
        list of items in required_list missing from search_list
    """

    missing_fields = [
        item for item in required_list if item not in search_list and item[0] != "$"
    ]
    if missing_fields:
        raise ValueError(
            f"{file_type} fields not found in data columns: {missing_fields}"
        )
    return missing_fields


def parse_args(args: dict):
    """
    Argument parser for command line apps

    Parameters
    ----------
    args : dict

    Returns
    -------
    vars(ap.parse_args(args=args))
        parsed arguments
    """
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-f",
        "--file_path",
        required=True,
        help=r"full path for data (eg. c:\Users\Joe\data\instruments1.csv)",
    )
    ap.add_argument(
        "-c",
        "--secrets_file",
        help=r"full path for credential secrets (eg. c:\Users\Joe\secrets.json). Not required if set as "
        r"environment variables",
    )
    ap.add_argument(
        "-m",
        "--mapping",
        required=True,
        help=r"full path to mappings.json (see mappings_template.json)",
    )
    ap.add_argument("-s", "--scope", help=r"LUSID scope to act in")
    ap.add_argument(
        "-ps", "--property_scope", help=r"LUSID scope to load properties into"
    )
    ap.add_argument(
        "-dl",
        "--delimiter",
        help=r"explicitly specify delimiter for data file and disable automatic delimiter detection",
    )
    ap.add_argument(
        "-nh",
        "--num_header",
        type=int,
        default=0,
        help="number of header lines before column titles",
    )
    ap.add_argument(
        "-nf",
        "--num_footer",
        type=int,
        default=0,
        help="number of footer lines after end of data",
    )
    ap.add_argument(
        "-lt",
        "--line_terminator",
        default=r"\n",
        help="character that specifies the end of a line, default value is {}".format(
            r"\n"
        ),
    )
    ap.add_argument(
        "-b",
        "--batch_size",
        default=2000,
        type=int,
        help="specifies the batch size for async requests",
    )
    ap.add_argument(
        "-disp",
        "--display_response_head",
        help="Displays the first 40 successful and unsuccessful items",
        action="store_true",
    )
    ap.add_argument(
        "-dr",
        "--dryrun",
        help="runs the app without calling LUSID",
        action="store_true",
    )
    ap.add_argument(
        "-d", "--debug", help=r"print debug messages, expected input: 'debug'"
    )

    return vars(ap.parse_args(args=args)), ap


def scale_quote_of_type(
    df: pd.DataFrame, mapping: dict, file_type: str = "quotes"
) -> (pd.DataFrame, dict):
    """
    Scales quote values of quotes of a specified type

    This function appends an extra row (__adjusted_quote) to a dataframe that contains quotes that have been scaled by
    a scale factor specified in the mapping, if they can be identified using another field. An example usage of this
    is processing of a quotes file containing a mixture of equities prices as GBP and GBp.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing quotes,
    mapping : dict
        mapping containing containing mapping[file_type]["quote_scalar"]
    file_type : str
        File type of data default = "quotes"

    Returns
    -------
    df : pd.DataFrame
        dataframe containing "__adjusted_quotes" column
    mapping : dict
        mapping updated with "metric_value.value" updated to be "__adjusted_quotes"
    """

    price_col = mapping[file_type]["quote_scalar"]["price"]
    type_col = mapping[file_type]["quote_scalar"]["type"]
    type_code = mapping[file_type]["quote_scalar"]["type_code"]
    scale_factor = mapping[file_type]["quote_scalar"]["scale_factor"]

    for col in [price_col, type_col]:
        if col not in df.columns:
            logging.error(f"column {col} does not exist in quotes DataFrame.")
            raise KeyError(f"column {col} does not exist in quotes DataFrame.")

    df["__adjusted_quote"] = None

    for index, row in df.iterrows():
        if np.isnan(row[price_col]) and row[type_col] == type_code:
            logging.warning(
                f"Could not adjust price at row {index} because it contains no price value"
            )
            continue
        elif np.isnan(row[price_col]):
            continue

        __adjusted_quote = (
            row[price_col] * scale_factor
            if row[type_col] == type_code
            else row[price_col]
        )

        df.at[index, "__adjusted_quote"] = __adjusted_quote
        mapping[file_type]["required"]["metric_value.value"] = "__adjusted_quote"
    return df, mapping


def identify_cash_items(
    dataframe, mappings, file_type: str, remove_cash_items: bool = False
) -> (pd.DataFrame, dict):
    """
    This function identifies cash items in a dataframe and either creates a currency_identifier in a new
    currency_identifier_for_LUSID column and amends the mapping dictionary accordingly or deletes cash items from the
    dataframe.

    Parameters
    ----------
    dataframe : pd.DataFrame
        The dataframe to look for cash items in
    mappings : dict
        Full mapping structure
    file_type : str
        type of data in dataframe eg. "instruments", "quotes", "transactions", "portfolios"
    remove_cash_items: bool
        indication to remove cash items from dataframe

    Returns
    -------
    dataframe : pd.DataFrame
        dataframe containing scaled quotes
    mappings : dict
        mapping with currency identifier mapping included
    """

    cash_flag_specification = mappings["cash_flag"]
    if not remove_cash_items:
        dataframe["__currency_identifier_for_LUSID"] = None
        mappings[file_type]["identifier_mapping"][
            "Currency"
        ] = "__currency_identifier_for_LUSID"

    rm_index = []
    for index, row in dataframe.iterrows():
        for column in cash_flag_specification["cash_identifiers"].keys():
            if row[column] in cash_flag_specification["cash_identifiers"][column]:
                if remove_cash_items:
                    rm_index.append(index)
                else:
                    dataframe.at[
                        index, "__currency_identifier_for_LUSID"
                    ] = populate_currency_identifier_for_LUSID(
                        row, column, cash_flag_specification
                    )
                break
    if remove_cash_items:
        dataframe = dataframe.drop(rm_index)

    return dataframe, mappings


def populate_currency_identifier_for_LUSID(
    row: dict, column, cash_flag_specification: dict
) -> str:
    """
    This function takes a cash transaction or holding in the form of a row from a dataframe and returns it's currency
    code, given the data's column containing a cash identifier and a dictionary that specifies how to set the currency
    code.

    Parameters
    ----------
    row : dict
        current data row
    column : str
        current dataframe column that contains values that can be used to identify a cash transaction or
        holding
    cash_flag_specification : dict
        dictionary containing cash identifier columns and values with either explicit currancy codes or the column from
        which the currency code can be infered

    Returns
    -------
    currency_code : str
        The currency code for the current transaction or holding
    """

    if isinstance(cash_flag_specification["cash_identifiers"][column], dict):
        if row[column] in cash_flag_specification["cash_identifiers"][column]:
            logging.debug("Getting currency code from explicit definition in mapping")
            currency_code = cash_flag_specification["cash_identifiers"][column][
                row[column]
            ]
            if not currency_code and "implicit" in cash_flag_specification.keys():
                logging.debug("couldn't find currency code in explicit definition. ")
                currency_code = row[cash_flag_specification["implicit"]]
        else:
            ex = (
                f"failed to find currency code either explicitly in cash_flag or implicitly from currency column "
                f"specified in cash_flag for {row}"
            )
            logging.error(ex)
            raise ValueError(ex)

    elif isinstance(cash_flag_specification["cash_identifiers"][column], list):
        if "implicit" in cash_flag_specification.keys():
            logging.info(
                "No currency codes explicitly specified, attempting to get implicitly from currency code "
                "column"
            )
            currency_code = row[cash_flag_specification["implicit"]]
        else:
            err = (
                f"No cash identifiers were specified as a list, without any explicit currency codes and no 'implicit'"
                f" field containing the name of a column containing currency codes exists. Please reformat cash_flag "
                f"inside mapping file correctly"
            )
            raise ValueError(err)
    else:
        logging.error(
            f"cash_flag not configured correctly. 'cash_identifiers' must be dictionary (explicit) or list "
            f"(for implicit), but  got {type(cash_flag_specification['cash_identifiers'])}"
        )
        raise ValueError(
            f"cash_flag not configured correctly. 'cash_identifiers' must be dictionary (explicit) or "
            f"list (for implicit), but  got {type(cash_flag_specification['cash_identifiers'])}"
        )
    return currency_code


def validate_mapping_file_structure(mapping: dict, columns: list, file_type: str):
    """
    This function takes a mapping structure and checks that each of the required fields is present

    Parameters
    ----------
    mapping : dict
        mapping containing full mapping structure
    columns : list
        columns from source data to search in
    file_type : str
        type of file being upserted eg. "instruments", "holdings", etc.

    Returns
    -------
    None

    """

    # file_type
    domain_lookup = load_json_file("config/domain_settings.json")
    file_type_check = (
        Validator(file_type, "file_type")
        .make_singular()
        .make_lower()
        .check_allowed_value(list(domain_lookup.keys()))
        .value
    )

    # required
    if "required" in mapping[file_type].keys():
        for field in mapping[file_type]["required"]:
            if isinstance(mapping[file_type]["required"][field], dict):
                check_mapping_fields_exist(
                    mapping[file_type]["required"][field]["column"].values(),
                    columns,
                    "required",
                )
            else:
                check_mapping_fields_exist(
                    mapping[file_type]["required"].values(), columns, "required"
                )
    else:
        raise ValueError(f"'required' mapping field not provided in mapping")

    # optional
    if "optional" in mapping.keys():
        check_mapping_fields_exist(
            mapping[file_type]["optional"].values(), columns, "optional"
        )

    # identifier_mapping
    if "identifier_mapping" in mapping[file_type].keys():
        check_mapping_fields_exist(
            mapping[file_type]["identifier_mapping"].values(),
            columns,
            "identifier_mapping",
        )


def strip_whitespace(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    This function removes prefixed or postfixed white space from string values in a Pandas DataFrame

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing data to remove whitespace from
    columns : list[dict{dict}]
        list of nested dictionaries of any depth

    Returns
    -------
    stripped_df : pd.DataFrame
        DataFrame with whitespace removed
    """

    stripped_df = pd.DataFrame.copy(df)

    for col in columns:
        stripped_df[col] = stripped_df[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )

    return stripped_df


def generate_time_based_unique_id(time_generator: None):
    """
    Generates a unique ID based on the time since epoch.

    Parameters
    ----------
    time_generator
        Any class that has a .time() method on it which produces time since 1970 in seconds

    Returns
    -------
    uid : str
        Unique, time based ID

    """

    if time_generator is None or isinstance(time_generator, types.ModuleType):
        time_generator = default_time

    elif getattr(time_generator, "time", None) is None or not isinstance(
        getattr(time_generator, "time"), types.MethodType
    ):
        raise AttributeError(
            "The provided time_generator does not have a method called time"
        )

    # Get the current time since epoch
    current_time = time_generator.time()

    if not isinstance(current_time, int) and not isinstance(current_time, float):
        raise ValueError(
            f"The provided response time time_generator.time() is not an int it is a {type(current_time)}"
        )

    # Multiply by 7 to get value to 100s of nano seconds
    timestamp = hex(int(current_time * 10000000.0))
    # Create the scope id by joining the hex representation with dashes every 4 characters
    uid = "-".join(timestamp[i : i + 4] for i in range(2, len(timestamp), 4))
    return uid


def generate_uuid():
    return str(uuid.uuid4())


def create_scope_id(time_generator=None, use_uuid=False):
    """
    This function creates a unique ID based on the time since epoch for use
    as a scope id.

    Parameters
    ----------
    time_generator
        Any class that has a .time() method on it which produces time since 1970 in seconds

    Returns
    -------
    scope_id : str
        Scope identifier
    """
    if use_uuid:
        return generate_uuid()
    else:
        return generate_time_based_unique_id(time_generator)


def default_fx_forward_model(
    df: pd.DataFrame,
    fx_code: str,
    func_transaction_units: typing.Callable[[], bool],
    func_total_consideration: typing.Callable[[], bool],
    mapping: dict,
) -> (pd.DataFrame, dict):
    """
    Function that takes 2 rows representing a single forward and merge them into a single transaction

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing transactions data
    fx_code : str
        The transaction type that identifies a forward
    func_transaction_units : typing.Callable[[], bool]
        function that evaluates to true for where the dataframe row contains transaction units
    func_total_consideration : typing.Callable[[], bool]
        function that evaluates to true for where the dataframe row contains total consideration
    mapping : dict
        mapping for FX transactions

    Returns
    -------
    fwds_txn_df : pd.DataFrame
        DataFrame containing FX transactions merged into a single row
    mapping_cash_txn : dict
        updates mapping dictionary for fwds_txn_df
    """

    logging.info(
        f"combining transactions of type {fx_code} into a single line using {default_fx_forward_model.__name__}"
        f" utility function"
    )

    t_type = mapping["transactions"]["required"]["type"]

    if fx_code not in df[t_type].values:
        raise ValueError(
            f"Input transactions have no fx transaction types {fx_code} in column transaction type{t_type}"
        )

    fwds_df = pd.DataFrame(df[df[t_type] == fx_code])

    transaction_units_df = fwds_df[func_transaction_units]
    total_consideration_df = fwds_df[func_total_consideration]

    t_id = mapping["transactions"]["required"]["transaction_id"]

    transaction_units_suffix = "_txn"
    total_consideration_suffix = "_tc"
    logging.info(
        f"merging buy and sell legs of FX trades and suffixing with {[transaction_units_suffix, total_consideration_suffix]}"
    )
    fwds_txn_df = pd.merge(
        transaction_units_df,
        total_consideration_df,
        how="outer",
        on=[t_id, t_type],
        suffixes=[transaction_units_suffix, total_consideration_suffix],
    )

    mapping_cash_txn = remap_after_merge(
        mapping,
        transaction_units_suffix=transaction_units_suffix,
        total_consideration_suffix=total_consideration_suffix,
    )

    return fwds_txn_df, mapping_cash_txn


def remap_after_merge(
    mapping: dict, transaction_units_suffix: str, total_consideration_suffix: str
) -> dict:
    """
    Remaps buy and sell fields in a mapping dictionary to the suffixed column names after a dataframe merge

    Parameters
    ----------
    mapping : dict
        mapping dictionary that needs updating
    transaction_units_suffix : str
        Suffix appended to transaction units transaction fields (e.g. "_txn")
    total_consideration_suffix : str
        Suffix appended to total consideration transaction fields(e.g. "_tc")

    Returns
    -------
    mapping : dict
        updated mapping dictionary
    """
    new_mapping = copy.deepcopy(mapping)
    file_type = "transactions"
    logging.info(f"updating mapping to new Total Consideration and transaction fields ")
    # currencies and amounts coming into the portfolio i.e. buy

    total_consideration_fields = [
        "total_consideration.amount",
        "total_consideration.currency",
        "settlement_currency",
    ]

    # currencies and amounts leaving the portfolio i.e. sell

    transaction_units_fields = ["units", "transaction_currency"]

    for key in new_mapping[file_type]["required"].keys():
        if key in transaction_units_fields:
            update_dict_value(
                new_mapping,
                key,
                new_mapping[file_type]["required"][key] + transaction_units_suffix,
                [file_type],
            )
        elif key in total_consideration_fields:
            update_dict_value(
                new_mapping,
                key,
                new_mapping[file_type]["required"][key] + total_consideration_suffix,
                [file_type],
            )
    return new_mapping


def update_dict_value(
    d: dict, s_key: str, val: typing.Union[str, float], top_level_values_to_search=[]
):
    """
    Recursively searches a dictionary for a key and updates the value

    This function searches for a key in a dictionary and updates the value belonging to any matching keys. The top level
    values in which to search can be specified as

    Parameters
    ----------
    d : dict
        Dictionary to update
    s_key : str
        Key to search for that belongs to the value to be updated
    val : typing.Union[str, float]
        Updated value belonging to search key
    file_type : str
        (optional) specific file_type in mapping to update. If not specified, all matches are replaced

    Returns
    -------
    d : dict
        updated dictionary

    """
    # if a file type had been specified, only search that values belonging to that key
    if top_level_values_to_search:
        for f_type in top_level_values_to_search:
            if f_type in d.keys():
                d[f_type] = update_dict_value(d.get(f_type, {}), s_key, val)
            else:
                err = (
                    f"file_type {top_level_values_to_search} not found in top level of mapping. If passing full mapping structure,"
                    f"ensure file type had been corrctly specified. If passing in a partial mapping structure,"
                    f"remove this parameter."
                )
                logging.error(err)
                raise KeyError(err)

    for k, v in d.items():
        # if no type specified and search key in keys
        if s_key in k:
            d[k] = update_value(d[k], val)
        # if no search key found, make recursive call for each key
        elif isinstance(v, dict) and not top_level_values_to_search:
            d[k] = update_dict_value(d.get(k, {}), s_key, val)
    return d


def update_value(d: typing.Union[dict, str], val: typing.Union[str, float]):
    """
    Updates value in dictionary and handles default and constant ($) specification

    Parameters
    ----------
    d : typing.Union[dict, str]
        Data key to update
    val : typing.Union[dict, str]
        value to update

    Returns
    -------
    None

    """

    # update values provided in "column" "default" format
    if isinstance(d, dict):
        if set(d.keys()) != {"column", "default"}:
            err = f"Failed to update dictionary. Expected ['column', 'default'] in {d}, but found {list(d.keys())}"
            raise ValueError(err)

        if type(val) != type(d["column"]):
            warn = f"new data type is not same as original value"
        #             logging.warning(warn)
        d["column"] = val
        return d

    if type(val) != type(d):
        warn = f"new data type is not same as original value"
    #         logging.warning(warn)

    # update value provided with constant format using "$"
    if isinstance(d, str) and d[0] == "$":
        return {"default": d[1:], "column": val}
    # for any other data types, simply update the value
    d = val

    return d


def group_request_into_one(
    model_type: str, request_list: list, attribute_for_grouping: list, batch_index=0
):
    """
    This function take a list of requests and collates an attribute from each request, adding the collated attributes
    back onto the first request in the list. The function returns the modified first request.
    For example, the function can take a list of CreatePortfolioGroupRequests, extract the "values" or portfolios from
    each request, and then add all portfolios back onto the first request in the list.

    Parameters
    ----------
    model_type : str
        the model type which we will modify (eg "CreatePortfolioGroupRequest").
    request_list : list
        a list of requests.
    attribute_for_grouping : list
        the attributes on these requests which will be grouped.
    batch_index
        The index of the batch

    Returns
    -------
    request
        a single LUSID request
    """

    # Define a base request for modifying - this is the first request in the list by default

    if model_type not in dir(models):
        raise ValueError(f"The model {model_type} is not a valid LUSID model.")

    if batch_index > len(request_list):
        raise IndexError(
            f"The length of the batch_index ({batch_index}) is greater than the request_list ({len(request_list)}) provided."
        )

    if type(attribute_for_grouping) == list and len(attribute_for_grouping) == 0:
        raise ValueError("The provided list of attribute_for_grouping is empty.")

    base_request = request_list[batch_index]

    for attrib in attribute_for_grouping:

        if "list" in getattr(models, model_type).openapi_types[attrib]:

            # Collect the attributes from each request onto a list

            batch_attrib = [
                lusid_model
                for nested_list in [
                    getattr(request, attrib)
                    for request in request_list
                    if getattr(request, attrib) is not None
                ]
                for lusid_model in nested_list
            ]

            # Assign collated values onto the base request

            setattr(base_request, attrib, batch_attrib)

        elif "dict" in getattr(models, model_type).openapi_types[attrib]:

            # Collect the attributes from each request onto a dictionary

            batch_attrib = dict(
                [
                    (lusid_model, nested_list[lusid_model])
                    for nested_list in [
                        getattr(request, attrib)
                        for request in request_list
                        if getattr(request, attrib) is not None
                    ]
                    for lusid_model in nested_list
                ]
            )

            # Assign collated values onto the base request

            setattr(base_request, attrib, batch_attrib)

    # Return base request with collated attributes

    return base_request
