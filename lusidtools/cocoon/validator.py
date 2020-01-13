import logging


class Validator:
    def __init__(self, value, value_name):
        self.value = value
        self.value_name = value_name

    def check_is_not_instance(self, instance_type):

        if isinstance(self.value, instance_type):
            raise TypeError(
                f"The {self.value_name} must be of type {str(instance_type)}, you supplied '{str(type(self.value))}' instead."
            )
        return self

    def check_allowed_value(self, allowed_values: list):
        """
        Checks that value exists in the provided list

        :param list allowed_values: The list of allowed values

        :return: self
        """
        if self.value not in allowed_values:
            raise ValueError(
                f"The {self.value_name} must be one of {str(allowed_values)}, you supplied '{self.value}' instead."
            )
        return self

    def make_singular(self):
        """
        Makes a plural string singular

        :return: self
        """
        if isinstance(self.value, str):
            self.value = self.value.rstrip("s")
            logging.debug(
                f"The value of {self.value_name} has had any 's' characters stripped from the right to make it singular"
            )
        return self

    def make_lower(self):
        """
        Makes a string lowercase

        :return: self
        """
        if isinstance(self.value, str):
            self.value = self.value.lower()
            logging.debug(f"The value of {self.value_name} has been made lower case")
        return self

    def set_default_value_if_none(self, default):
        """
        Sets a default value if the current value is None

        :param default: The default value

        :return: self
        """
        if self.value is None:
            self.value = default
            logging.debug(
                f"The value of {self.value_name} has been updated from None to {default}"
            )
        return self

    def override_value(self, override_flag: bool, override_value):
        """
        Overrides the current value of the ovverride_flag is True

        :param bool override_flag: Whether or not to override the current value
        :param override_value: The value to use to override the existing value

        :return: self
        """
        if override_flag:
            self.value = override_value
            logging.debug(
                f"The value of {self.value_name} has been overriden with {override_value}"
            )
        return self

    def discard_dict_keys_none_value(self):
        """
        Discards dictionary key, value pairs where the value is None

        :return: self
        """
        if isinstance(self.value, dict):
            invalid_keys = [key for key, value in self.value.items() if value is None]
            if len(invalid_keys) > 0:
                logging.info(
                    f"The values for the keys {str(invalid_keys)} are None and have thus been removed from {self.value_name}"
                )
                for key in invalid_keys:
                    self.value.pop(key, None)
        return self

    def get_dict_values(self):
        """
        Gets a list of values from a dictionary

        :return: self
        """
        if isinstance(self.value, dict):
            self.value = list(self.value.values())
        return self

    def filter_list_using_first_character(self, first_character: str):
        """
        Filters a list of strings by looking to see if the first character matches the provided

        :param str first_character: The character to look for in the first character of each element
        :return: self
        """
        if isinstance(self.value, list):
            to_remove = []
            for value in self.value:
                if isinstance(value, str) and value[0] == first_character:
                    to_remove.append(value)
            self.value = [value for value in self.value if value not in to_remove]
        return self

    def check_subset_of_list(self, superset: list, superset_name: str):
        """
        Checks if one list is a subset of another

        :param list superset: The superset to check of the value is a subset of
        :param list superset_name: The name of the superset

        :return: self
        """
        if isinstance(self.value, list):
            if len(set(self.value) - set(superset)) > 0:
                raise ValueError(
                    f"""The values {str(set(self.value) - set(superset))} exist in the {self.value_name}
                                   but do not exist in the {superset_name}."""
                )
        return self

    def check_no_intersection_with_list(self, other_list: list, list_name: str):
        """
        Checks that the value has no intersection with a provided list

        :param list other_list: The list to check the value has no intersection with
        :param str list_name: The name of the list

        :return: self
        """
        if isinstance(self.value, list):
            if len(set(other_list).intersection(set(self.value))) > 0:
                err = f"""The columns {str(set(other_list).intersection(set(self.value)))} are specified in {self.value_name}
                                     yet they contain null (NaN) values for some rows in the provided data. Null values are not 
                                     allowed for required fields. Please ensure that required columns do not contain ANY null 
                                     values or specify a default value in your mapping by specifying a dictionary with the keys
                                     "column" and "default"."""
                logging.error(err)
                raise ValueError(err)
