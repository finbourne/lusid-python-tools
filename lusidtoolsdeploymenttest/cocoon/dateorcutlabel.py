import numpy as np
import pandas as pd
from dateutil import parser
from datetime import datetime
import pytz
import re
from collections import UserString


def _process_timestamp(datetime_value: pd.Timestamp):
    """
    Processes pandas timestamp convert it to ISO format and parse to string

    Parameters
    ----------
    datetime : pd.Timestamp
        Datetime value

    Returns
    -------
    datetime : pd.Timestamp
        Datetime value in ISO format
    """
    return pd.to_datetime(datetime_value, utc=True, unit="us").isoformat()


def _process_custom_date(datetime_value: str, date_format: str) -> str:
    """
    Processes a datetime provided in custom format

    Parameters
    ----------
    datetime_value : str
        Custom Datetime value
    date_format : str
        Format of custom Datetime

    Returns
    -------
    datetime_value : str
        Datetime value as str
    """
    if not isinstance(datetime_value, str):
        raise TypeError(
            f"Date {datetime_value} is of type {type(datetime_value)} must be of type 'str' "
            f"when specifying a custom date format. "
        )

    try:
        datetime_value = datetime.strptime(datetime_value, date_format).isoformat()
    except ValueError:
        raise ValueError(
            f"The date format provided {date_format} was not recognised in the"
            f" datetime provided: {datetime_value}"
        )

    return datetime_value


def _process_date_as_string(datetime_value: str):
    """
    Adds timezone to partially ISO valid datetimes as strings

    Parameters
    ----------
    datetime_value : str
        Datetime value provided as a string

    Returns
    -------
    datetime_value : str
        Datetime value provided as a string in valid ISO format

    """
    # Cut label regular expression, no modification required
    if re.findall("\d{4}-\d{2}-\d{2}N\w+", datetime_value):
        pass

    # Already in isoformat and UTC timezone
    elif re.findall(
        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", datetime_value
    ) or re.findall("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z", datetime_value):
        pass

    # Already in isoformat but not necessarily UTC timezone
    elif re.findall(
        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[\+-]\d{2}:\d+", datetime_value
    ) or re.findall(
        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+[\+-]\d{2}:\d{2}", datetime_value
    ):
        # Convert to UTC
        datetime_value = (
            parser.isoparse(datetime_value).astimezone(pytz.utc).isoformat()
        )

    # ISO format with no timezone
    elif re.findall("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", datetime_value):
        datetime_value = datetime_value + "+00:00"
    elif re.findall("\d{4}-\d{2}-\d{2}", datetime_value):
        datetime_value = datetime_value + "T00:00:00+00:00"
    else:
        datetime_value = _process_datetime(
            parser.parse(timestr=datetime_value, dayfirst=True)
        )
    return datetime_value


def _process_numpy_datetime64(datetime_value: np.datetime64) -> str:
    """
    Converts numpy.datetime64 to UTC date to a string

    Parameters
    ----------
    datetime_value : np.datetime64
        Datetime value

    Returns
    -------
    datetime_value : str
        timezone aware UTC date

    """
    return str(np.datetime_as_string(arr=datetime_value, timezone="UTC", unit="us"))


def _process_ndarray(datetime_value: np.ndarray) -> str:
    """
    Converts numpy.ndarray to UTC date to a string

    Parameters
    ----------
    datetime_value : np.ndarray
        Datetime value

    Returns
    -------
    datetime_value : str
        timezone aware UTC date

    """
    return str(np.datetime_as_string(arr=datetime_value, timezone="UTC", unit="us")[0])


def _process_datetime(datetime_value):
    # If there is no timezone assume that it is in UTC
    if (
        datetime_value.tzinfo is None
        or datetime_value.tzinfo.utcoffset(datetime_value) is None
    ):
        return datetime_value.replace(tzinfo=pytz.UTC).isoformat()
    # If there is a timezone convert to UTC
    else:
        return datetime_value.astimezone(pytz.UTC).isoformat()


class DateOrCutLabel(UserString):
    def __init__(self, datetime_value, date_format=None):
        def convert_datetime_utc(datetime_value, date_format=None):
            """
            This function ensures that a variable is a timezone aware UTC datetime

            Parameters
            ----------
            datetime_value : any
                Datetime variable
            date_format : str
                (optional)The format of a custom date as a string eg "%Y-%m-%d %H:%M:%S.%f". see https://strftime.org/

            Returns
            -------
            datetime_value : str
                The converted timezone aware datetime in the UTC timezone

            """

            # Convert custom dates to readable string format
            if date_format:
                datetime_value = _process_custom_date(datetime_value, date_format)

            # Convert strings to readable string format
            if isinstance(datetime_value, str):
                return _process_date_as_string(datetime_value)

            # Convert datetime to string
            elif isinstance(datetime_value, datetime):
                return _process_datetime(datetime_value)

            # Convert np.datetime to string
            elif isinstance(datetime_value, np.datetime64):
                return _process_numpy_datetime64(datetime_value)

            # Convert np.ndarray to string
            elif isinstance(datetime_value, np.ndarray):
                return _process_ndarray(datetime_value)

            return datetime_value

        self.data = convert_datetime_utc(datetime_value, date_format)
