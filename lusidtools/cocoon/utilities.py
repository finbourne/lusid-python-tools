import argparse
import csv
import logging
import os
import numpy as np
import lusid
from collections.abc import Mapping
import pandas as pd
from dateutil import parser
from datetime import datetime
from detect_delimiter import detect
import requests
import json
import pytz
import re
import inspect
import functools
from pathlib import Path
import re
from lusidtools.cocoon.dateorcutlabel import DateOrCutLabel
import logging

from lusidtools.cocoon.validator import Validator
from lusidtools.logger import LusidLogger


def convert_datetime_utc(datetime_value) -> datetime:
    """
    This function ensures that a variable is a timezone aware UTC datetime

    :param any datetime_value:
    :return: datetime datetime_value: The converted timezone aware datetime in the UTC timezone
    """

    # Add regular expression check for ISO format

    # If the datetime is a string try and parse it
    if isinstance(datetime_value, str):
        datetime_value = parser.parse(timestr=datetime_value, dayfirst=True)

    # if the datetime has been parsed from a string or is already a datetime
    if isinstance(datetime_value, datetime):
        # If there is no timezone assume that it is in UTC
        if (
            datetime_value.tzinfo is None
            or datetime_value.tzinfo.utcoffset(datetime_value) is None
        ):
            return datetime_value.replace(tzinfo=pytz.UTC)
        # If there is a timezone convert to UTC
        else:
            return datetime_value.astimezone(pytz.UTC)

    return datetime_value


def make_code_lusid_friendly(raw_code):
    """
    This function takes a column name and converts it to a LUSID friendly code for us in creating LUSID objects

    :param str raw_code: A raw column header which needs special characters stripped out
    :return: str friendly_code: A LUSID friendly code with special characters removed
    """
    max_length = 64

    if len(raw_code) > max_length:
        raise ValueError(
            f"""The name {raw_code} is {len(raw_code)} characters long and exceeds the limit of {max_length}
                             for a code. Please shorten it by {len(raw_code) - 64} characters."""
        )

    friendly_code = (
        str(raw_code)
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "")
        .replace("%", "Percentage")
        .replace("&", "and")
        .replace(".", "_")
        .strip()
    )

    return friendly_code


def populate_model(
    model_object,
    required_mapping,
    optional_mapping,
    row,
    properties=None,
    identifiers=None,
):
    """
    This function populates

    :param lusid.models model_object: The class of the model object to populate
    :param required_mapping:
    :param optional_mapping:
    :param row:
    :param properties:
    :param identifiers:
    :return:
    """

    if getattr(lusid.models, model_object.__name__) is None:
        raise TypeError("The provided model_object is not a lusid.model object")

    mapping_expanded = expand_dictionary(
        update_dict(required_mapping, optional_mapping)
    )

    return set_attributes(
        model_object=model_object,
        mapping=mapping_expanded,
        row=row,
        properties=properties,
        identifiers=identifiers,
        sub_holding_keys=None,
    )


def set_attributes(
    model_object, mapping, row, properties=None, identifiers=None, sub_holding_keys=None
):
    """
    This function takes a lusid.model object and an expanded mapping between its attributes and

    :param lusid.models model_object: The class of the model object to populate
    :param dict mapping: The expanded dictionary mapping the Series columns to the LUSID model attributes
    :param pd.Series row: The current row of the DataFrame being worked on
    :param any properties: The properties to use on this model
    :param any identifiers: The instrument identifiers to use on this model
    :param list[str] sub_holding_keys: The sub holding keys to use on this model
    :return: lusid.models new model_object: An instance of the model object with populated attributes
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
        # This block keeps track of the number of missing (non-additional) attributes, if they are all none it will return None
        else:
            total_count += 1
            if mapping[key] is None:
                none_count += 1

        # Get the attribute type
        attribute_type = obj_attr[key]

        # If this is the last object and there is no more nesting set the value from the row
        if not isinstance(mapping[key], dict):
            if mapping[key] is not None and not pd.isna(row[mapping[key]]):
                # Need an explicit datetime or cutlabel type in place of this
                if "date" in key or "created" in key:
                    obj_init_values[key] = str(DateOrCutLabel(row[mapping[key]]))
                else:
                    obj_init_values[key] = row[mapping[key]]

        # if there is more nesting call the function recursively
        else:
            # Temporary workaround, need to add a more robust solution here
            if "list[" in attribute_type:
                attribute_type = attribute_type.split("[")[1].replace("]", "")

                obj_init_values[key] = [
                    set_attributes(
                        model_object=getattr(lusid.models, attribute_type),
                        mapping=mapping[key],
                        row=row,
                    )
                ]

            else:
                obj_init_values[key] = set_attributes(
                    model_object=getattr(lusid.models, attribute_type),
                    mapping=mapping[key],
                    row=row,
                )

    """
    If all attributes are None propagate None rather than a model filled with Nones. For example if a CorporateActionSourceId
    has no scope or code return build a model with CorporateActionSourceId = None rather than CorporateActionSourceId = 
    lusid.models.ResourceId(scope=None, code=None)
    """
    if total_count == none_count:
        return None

    # Create an instance of and populate the model object
    return model_object(**obj_init_values)


def update_dict(orig_dict, new_dict) -> dict:
    """
    This is used to update a dictionary with another dictionary. Using the default Python update method does not merge
    nested dictionaries. This method allows for this.

    :param dict orig_dict: The original dictionary to update
    :param dict new_dict: The new dictionary to merge with the original
    :return: dict orig_dict: The updated original dictionary
    """

    for key, val in new_dict.items():
        if isinstance(val, Mapping):
            tmp = update_dict(orig_dict.get(key, {}), val)
            orig_dict[key] = tmp
        elif isinstance(val, list):
            orig_dict[key] = orig_dict.get(key, []) + val
        else:
            orig_dict[key] = new_dict[key]
    return orig_dict


def expand_dictionary(dictionary) -> dict:
    """
    Takes a flat dictionary (no nesting) with keys separated by a separator "." and converts it into a nested
    dictionary

    :param dict dictionary: The input dictionary with separated keys
    :return: dict dict_expanded: The expanded nested dictionary
    """

    dict_expanded = {}

    # Loop over each composite key and final value
    for key, value in dictionary.items():
        # Split the key on the separator
        components = key.split(".")
        # Get the expanded dictionary for this key and update the master dictionary
        update_dict(
            dict_expanded, expand_dictionary_single_recursive(0, components, value)
        )

    return dict_expanded


def expand_dictionary_single_recursive(index, key_list, value) -> dict:
    """
    Takes a list of keys and a value and turns it into a nested dictionary. This is a recursive function.

    :param int index: The current index of the key in the list of keys
    :param list[str] key_list: The list of keys to turn into a nested dictionary
    :param any value: The final value to match against the last (deepest) key
    :return: dict: The nested dictionary for the current key in the list
    """

    key = key_list[index]
    if key == key_list[-1]:
        return {key: value}

    return {key: expand_dictionary_single_recursive(index + 1, key_list, value)}


def get_swagger_dict(api_url) -> dict:
    """
    Gets the lusid.json swagger file

    :param str api_url: The base api url for the LUSID instance
    :return: dict: The swagger file as a dictionary
    """
    swagger_path = "/swagger/v0/swagger.json"
    swagger_url = api_url + swagger_path
    swagger_file = requests.get(swagger_url)

    if swagger_file.status_code == 200:
        return json.loads(swagger_file.text)
    else:
        raise ValueError(
            f"""Received a {swagger_file.status_code} response from the provided url, please double check
                             the base api url and try again"""
        )


def verify_all_required_attributes_mapped(
    swagger_dict, mapping, model_object, exempt_attributes=[]
) -> None:
    """
    Verifies that all required attributes are included in the mapping, passes silently if they are and raises an exception
    otherwise

    :param dict swagger_dict: The full LUSID swagger dictionary
    :param dict mapping: The required mapping
    :param lusid.model model_object: The LUSID model that the mapping applies to
    :param list[str] exempt_attributes: The attributes that are exempt from needing to be in the required mapping

    :return: None
    """
    required_attributes = get_required_attributes_model_recursive(
        swagger_dict=swagger_dict, model_object=model_object
    )

    for attribute in required_attributes:
        if attribute.split(".")[0] in exempt_attributes:
            required_attributes.remove(attribute)

    if mapping is None:
        raise ValueError(
            f"""No required mapping has been provided. The required attributes {str(required_attributes)} 
                             are missing from the mapping. Please add them to a mapping and provide it."""
        )

    missing_attributes = set(required_attributes) - set(list(mapping.keys()))

    if len(missing_attributes) > 0:
        raise ValueError(
            f"""The required attributes {str(missing_attributes)} are missing from the mapping. Please
                             add them."""
        )


def generate_required_attributes_list():
    pass


def get_required_attributes_model_recursive(swagger_dict, model_object):
    """
    This is a recursive function which gets all of the required attributes on a LUSID model. If the model is nested
    then it separates the attributes by a '.' until the bottom level where no more models are required and a primitive
    type is supplied e.g. string, int etc.

    :param dict swagger_dict: The LUSID swagger.json as a dictionary
    :param lusid.model model_object: The model to get required attributes for

    :return: list[str]: The required attributes of the model
    """
    attributes = []

    # If there are required attributes collect them as a list
    if "required" in list(swagger_dict["definitions"][model_object.__name__].keys()):
        required_attributes = swagger_dict["definitions"][model_object.__name__][
            "required"
        ]
    # If there are no required attributes on a model, assume that all attributes are required
    # This is for cases such as lusid.models.TransactionRequest.transaction_price
    else:
        required_attributes = list(
            swagger_dict["definitions"][model_object.__name__]["properties"].keys()
        )

    # Get the properties of all the attributes on the model
    attribute_properties = swagger_dict["definitions"][model_object.__name__][
        "properties"
    ]

    for required_attribute in required_attributes:
        nested_model = return_nested_model(attribute_properties[required_attribute])

        if nested_model is None:
            attributes.append(camel_case_to_pep_8(required_attribute))

        else:
            nested_required_attributes = get_required_attributes_model_recursive(
                swagger_dict=swagger_dict,
                model_object=getattr(lusid.models, nested_model),
            )

            for nested_required_attribute in nested_required_attributes:
                attributes.append(
                    ".".join(
                        [
                            camel_case_to_pep_8(required_attribute),
                            nested_required_attribute,
                        ]
                    )
                )

    return attributes


def return_nested_model(required_attribute_properties):
    """
    Takes the properties of a required attribute on a model and searches as to whether or not this attribute
    requires a model of its own

    :param dict required_attribute_properties: The properties of the required attribute

    :return: str: The name of the LUSID model
    """
    possible_nested_models = list(
        gen_dict_extract("$ref", required_attribute_properties)
    )

    if len(possible_nested_models) > 0:
        return possible_nested_models[0].split("/")[2]
    else:
        return None


def gen_dict_extract(key, var):
    """
    Searches a nested dictionary for a key, yielding any values it finds against that key

    :param key: The key to search for
    :param var: The dictionary to search

    :return: generator(result): A generator with the results
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


def camel_case_to_pep_8(attribute_name) -> str:
    """
    Converts a camel case name to PEP 8 standard

    :param attribute_name: The camel case attribute name
    :return: str: The PEP 8 formatted attribute name
    """
    matches = re.finditer(
        ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", attribute_name
    )
    return "_".join([m.group(0)[0].lower() + m.group(0)[1:] for m in matches])


def convert_cell_value_to_string(data):
    """
    Converts the value of a call to a string if it is a list or a dictionary

    :param data: The value of the cell in the dataframe
    :return: str: The original data if it is not a list or a dictionary, otherwise the string representation of these
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

    :param pd.DataFrame data_frame: The updated dataframe
    :param dict mapping: The original mapping (can be required or optional)
    :param str constant_prefix: The prefix that can be used to specify a constant

    :return: pd.DataFrame dict dataframe mapping_updated: The updated DataFrame and mapping
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
            if value[0] != constant_prefix:
                mapping_updated[key] = value
            else:
                mapping_updated[key] = f"LUSID.{key}"
                data_frame[mapping_updated[key]] = value[1:]

        else:
            raise ValueError(
                f"""You have passed in a value with type {type(value)} for the mapping for {key}, this is
                                 not a supported type. Please provide a string with the column name to use, a constant
                                 value prefixed by {constant_prefix} or a dictionary
                                 with the keys "column" and "default" where column is the column name and default
                                 being the default value to use."""
            )

    return data_frame, mapping_updated


def checkargs(function):
    """
    This can be used as a decorator to test the type of arguments are correct

    :param function: The function to wrap with annotated types, all parameters must be annotated with a type

    :return:
    """

    @functools.wraps(function)
    def _f(*args, **kwargs):

        # Get all the function arguments in order
        function_arguments = inspect.signature(function).parameters

        # For each non keyword argument provided key it by the argument name
        keyed_arguments = {
            list(function_arguments.keys())[i]: args[i] for i in range(0, len(args))
        }

        # Update this with the keyword arguments
        keyed_arguments.update(kwargs)

        # For each argument raise an error if it is of the incorrect type and if it has a default it is not the
        # default value
        for argument_name, argument_value in keyed_arguments.items():

            if argument_name not in list(function_arguments.keys()):
                raise ValueError(
                    f"The argument {argument_name} is not a valid keyword argument for this function, valid arguments are {str(list(function_arguments.keys()))}"
                )

            argument = function_arguments[argument_name]
            is_default_value = False

            if argument.default is not argument.empty:
                if argument.default is None:
                    is_default_value = argument_value is argument.default
                else:
                    is_default_value = argument_value == argument.default

            if not isinstance(argument_value, argument.annotation):
                if not is_default_value:
                    raise TypeError(
                        f"""The value provided for {argument_name} is of type {type(argument_value)} not of 
                    type {argument.annotation}. Please update the provided value to be of type 
                    {argument.annotation}"""
                    )

        return function(*args, **kwargs)

    return _f


def load_json_file(relative_file_path: str) -> dict:
    """

    :param str relative_file_path: path to json file
    :return: dict data: parsed data from json file
    """

    file_path = Path(__file__).parent.joinpath(relative_file_path)
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data


@checkargs
def load_data_to_df_and_detect_delimiter(args: dict) -> pd.DataFrame:
    """
    This function loads data from given file path and converts it into a pandas DataFrame
    :param dict args: Arguments parsed in from command line, containing
    :return: pandas.DataFrame: dataframe Containing data from given file path
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
    :param str file_type:
    :param list[str] required_list: List of items to search for
    :param list[str] search_list:  list to search in
    :return: list[str] missing_fields: list of items in required_list missing from search_list
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
    )  # TODO: create support article on mapping.json structure
    ap.add_argument("-s", "--scope", default=None, help=r"LUSID scope to act in")
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
    ap.add_argument("-dr", "--dryrun", action="store_true")
    ap.add_argument(
        "-d", "--debug", help=r"print debug messages, expected input: 'debug'"
    )

    return vars(ap.parse_args(args=args)), ap


def scale_quote_of_type(
    df: pd.DataFrame, mapping: dict, file_type: str = "quotes"
) -> pd.DataFrame:
    """

    :param string file_type: File type of data default = "quotes"
    :param pd.DataFrame df: DataFrame containing data to be scaled
    :param mapping: mapping configuration containing quote_scalar dictionary
    :return:
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
    return df


def identify_cash_items(
    dataframe, mappings, file_type: str, remove_cash_items: bool = False
) -> (pd.DataFrame, dict):
    """
    This function identifies cash items in a dataframe and either creates a currency_identifier in a new
    currency_identifier_for_LUSID column and amends the mapping dictionary accordingly or deletes cash items from the
    dataframe.

    :param pd.DataFrame dataframe: dataframe to look for cash items in
    :param dict mappings: Full mapping structure
    :param str file_type: type of data in dataframe ["instruments", "quotes", "transactions", "portfolios"]
    :param bool remove_cash_items: indication to remove cash items from dataframe
    :return: pd.DataFrame dataframe: dataframe containing
    :return: dict mappings: mapping with currency identifier mapping included
    """

    cash_flag_specification = mappings["cash_flag"]
    dataframe["__currency_identifier_for_LUSID"] = None
    if not remove_cash_items:
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

    :param dict row: current data row
    :param column: current dataframe column that contains values that can be used to identify a cash transaction or
    holding
    :param dict cash_flag_specification: dictionary containing cash identifier columns and values with either explicit
    currancy codes or the column from which the currency code can be infered
    :return: str currency_code:The currency code for the current transaction or holding
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
    This function takes a mapping structure and checks that each of the
    :param dict mapping: mapping containing full mapping structure
    :param columns: columns from source data to search in
    :param file_type:
    :return:
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
    else:
        raise ValueError(f"'identifier_mapping' mapping field not provided in mapping")


def strip_whitespace(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    This function removes prefixed or postfixed white space from string values in a Pandas DataFrame

    :param pd.Dataframe df: Dataframe containing data to remove whitespace from
    :param list[dict{dict}] columns: list of nested dictionaries of any depth
    :return: pd.Dataframe stripped_df: DataFrame with whitespace removed
    """
    stripped_df = pd.DataFrame.copy(df)

    for col in columns:
        stripped_df[col] = stripped_df[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )

    return stripped_df
