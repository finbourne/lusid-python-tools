import numpy as np
import pandas as pd
from dateutil import parser
from datetime import datetime
import pytz
import re
from collections import UserString


class DateOrCutLabel(UserString):
    def __init__(self, datetime_value, custom_date_format=None):
        def convert_datetime_utc(datetime_value, custom_date_format=None):
            """
            This function ensures that a variable is a timezone aware UTC datetime

            :param any datetime_value:
            :return: datetime datetime_value: The converted timezone aware datetime in the UTC timezone
            """
            if custom_date_format:
                if not isinstance(datetime_value, str):
                    raise TypeError(f"Date {datetime_value} is of type {type(datetime_value)} must be of type 'str' "
                                    f"when specifying a custom date format. ")

                try:
                    datetime_value = pd.to_datetime(datetime_value, format=custom_date_format,
                                                    utc=False)
                except ValueError:
                        raise ValueError(f"The date format provided {custom_date_format} was not recognised in the"
                                         f" datetime provided: {datetime_value}")

                def formatdt(dt):
                    return str(
                                    np.datetime_as_string(
                                        arr=np.datetime64(dt.strftime("%Y-%m-%d %H:%M:%S.%f")), timezone="UTC", unit="us"
                                    )
                                )
                datetime_value = formatdt(datetime_value)

            # If the datetime is a pandas timestamp convert it to ISO format and parse to string
            if isinstance(datetime_value, pd.Timestamp):
                datetime_value = pd.to_datetime(
                    datetime_value, utc=True, unit="us"
                ).isoformat()

            # If the datetime is a string try and parse it
            if isinstance(datetime_value, str):
                # Cut label regular expression, no modification required
                if re.findall("\d{4}-\d{2}-\d{2}N\w+", datetime_value):
                    pass
                # Already in isoformat
                elif (
                    re.findall(
                        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d+",
                        datetime_value,
                    )
                    or re.findall(
                        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", datetime_value
                    )
                    or re.findall(
                        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z", datetime_value
                    )
                    or re.findall(
                        "\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+\+\d{2}:\d{2}",
                        datetime_value,
                    )
                ):
                    pass
                # ISO format with no timezone
                elif re.findall("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", datetime_value):
                    datetime_value = datetime_value + "+00:00"
                elif re.findall("\d{4}-\d{2}-\d{2}", datetime_value):
                    datetime_value = datetime_value + "T00:00:00+00:00"
                else:
                    datetime_value = parser.parse(timestr=datetime_value, dayfirst=True)

            # if the datetime has been parsed from a string or is already a datetime
            if isinstance(datetime_value, datetime):
                # If there is no timezone assume that it is in UTC
                if (
                    datetime_value.tzinfo is None
                    or datetime_value.tzinfo.utcoffset(datetime_value) is None
                ):
                    return datetime_value.replace(tzinfo=pytz.UTC).isoformat()
                # If there is a timezone convert to UTC
                else:
                    return datetime_value.astimezone(pytz.UTC).isoformat()


            # If datetime is numpy datetime convert to ISO format and parse to string
            if isinstance(datetime_value, np.datetime64):
                datetime_value = str(
                    np.datetime_as_string(
                        arr=datetime_value, timezone="UTC", unit="us"
                    )
                )
                print(datetime_value)


            # If datetime is numpy datetime convert to ISO format and parse to string
            if isinstance(datetime_value, np.ndarray):
                datetime_value = str(
                    np.datetime_as_string(
                        arr=datetime_value, timezone="UTC", unit="us"
                    )[0]
                )
            return datetime_value

        self.data = convert_datetime_utc(datetime_value, custom_date_format)
