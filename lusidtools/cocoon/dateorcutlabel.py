import numpy as np
import pandas as pd
from dateutil import parser
from datetime import datetime
import pytz
import re
from collections import UserString


class DateOrCutLabel(UserString):
    def __init__(self, datetime_value):
        def convert_datetime_utc(datetime_value):
            """
            This function ensures that a variable is a timezone aware UTC datetime

            :param any datetime_value:
            :return: datetime datetime_value: The converted timezone aware datetime in the UTC timezone
            """

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
            if isinstance(datetime_value, np.ndarray):
                datetime_value = str(
                    np.datetime_as_string(
                        arr=datetime_value, timezone="UTC", unit="us"
                    )[0]
                )
            return datetime_value

        self.data = convert_datetime_utc(datetime_value)
