import yaml
from typing import Callable
from lusid import models

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# Common abbreviations
rev_abbrev = {
    "FIFO": "TransactionConfiguration/default/TaxLotSelectionMethod=FirstInFirstOut",
    "LIFO": "TransactionConfiguration/default/TaxLotSelectionMethod=LastInFirstOut",
    "HCF": "TransactionConfiguration/default/TaxLotSelectionMethod=HighestCostFirst",
    "LCF": "TransactionConfiguration/default/TaxLotSelectionMethod=LowestCostFirst",
    "AC": "TransactionConfiguration/default/TaxLotSelectionMethod=AverageCost",
    "LL": "LongLonger",
    "SL": "ShortLonger",
    "LS": "LongShorter",
    "SS": "ShortShorter",
    "S1": "Side1",
    "S2": "Side2",
    "STOCK": "Settlement, Traded",
    "CASH": "Commitment, CashSettlement",
    "FWD": "ForwardFx, CashSettlement",
    "RCV": "Receivable, CashSettlement",
}

# Reverse key map for lookups
fwd_abbrev = {v: k for k, v in rev_abbrev.items()}


# Methods to map between abbreviations
def abbrev(v):
    return fwd_abbrev.get(v, v)


def unabbrev(v):
    return rev_abbrev.get(v, v)


# Dumper class with the custom constructor/resolvers
class CustomDumper(yaml.Dumper):
    pass


# Loader class with the custom constructor for querying
# This is required because the input message uses different classes
class QueryLoader(yaml.Loader):
    pass


# Loader class with the custom constructor for updates
# This is required because the input message uses different classes
class UpdateLoader(yaml.Loader):
    pass


class TxnConfigYaml:
    """
    This class holds all the configuration and methods for working with the TransactionTypeConfiguration

    Each object in the TransactionTypeConfiguration has a representation method which converts a LUSID model to YAML
    with the suffix "_rep" and a constructor method which converts YAML to a LUSID model with the suffix "_con".
    """

    def __init__(
        self,
        models,
        UpdateLoader: yaml.Loader = UpdateLoader,
        QueryLoader: yaml.Loader = QueryLoader,
        CustomDumper: yaml.Dumper = CustomDumper,
    ):
        """
        The initialisation here will set up the custom constructors and representers. It will add these to the the
        CustomDumper, QueryLoader and UpdateLoader.

        :param models: The lusid.models
        :param yaml.Loader UpdateLoader: The loader to use for updating (writing to LUSID)
        :param yaml.Loader QueryLoader: The loader to use for querying (reading from LUSID)
        :param yaml.Dumper CustomDumper: The dumper used for converting LUSID models to YAML
        """
        self.YAMLConverter(
            "!Txn",
            self.config_rep,
            self.TransactionSetConfigurationDataNoLinks,
            self.config_con,
            self.TransactionSetConfigurationDataNoLinks,
            models.TransactionSetConfigurationDataRequest,
        )

        self.YAMLConverter(
            "!TxnConfig",
            self.trans_config_rep,
            models.TransactionConfigurationData,
            self.trans_config_con,
            models.TransactionConfigurationData,
            models.TransactionConfigurationDataRequest,
        )

        self.YAMLConverter(
            "!Mvmt",
            self.mvmt_rep,
            models.TransactionConfigurationMovementData,
            self.mvmt_con,
            models.TransactionConfigurationMovementData,
            models.TransactionConfigurationMovementDataRequest,
        )

        self.YAMLConverter(
            "!Map",
            self.pm_rep,
            models.TransactionPropertyMapping,
            self.pm_con,
            models.TransactionPropertyMapping,
            models.TransactionPropertyMappingRequest,
        )

        self.YAMLConverter(
            "!Alias",
            self.alias_rep,
            models.TransactionConfigurationTypeAlias,
            self.alias_con,
            models.TransactionConfigurationTypeAlias,
            models.TransactionConfigurationTypeAlias,
        )

        self.YAMLConverter(
            "!SideConfig",
            self.side_config_rep,
            models.SideConfigurationData,
            self.side_config_con,
            models.SideConfigurationData,
            models.SideConfigurationDataRequest,
        )

        self.YAMLConverter(
            "!Prop",
            self.pp_rep,
            models.PerpetualProperty,
            self.pp_con,
            models.PerpetualProperty,
            models.PerpetualProperty,
        )

    class YAMLConverter:
        """
        This class handles the configuration for a single element of the model e.g. aliases on a
        TransactionTypeConfiguration.
        """

        def __init__(
            self,
            tag: str,
            representation_function: Callable,
            representation_model,
            constructor_function: Callable,
            constructor_read_model,
            constructor_update_model,
            representation_dumper: yaml.Dumper = CustomDumper,
            constructor_read_loader: yaml.Loader = QueryLoader,
            constructor_update_loader: yaml.Loader = UpdateLoader,
        ):
            """
            The initialisation function creates the representation and constructor functions from the provided
            base callable functions and registers them with the dumper and loaders.

            :param str tag: The tag to use for this node
            :param Callable representation_function: The base representation function to use when converting to YAML
            :param lusid.models representation_model: The model to convert from when representing as YAML
            :param constructor_function: The base constructor function to use when converting from YAML
            :param lusid.models constructor_read_model: The model to use for reading from LUSID
            :param lusid.models constructor_update_model: The model to use for sending to LUSID
            (usually has "Request" added to the name)
            :param yaml.Dumper representation_dumper: The dumper to use for converting to YAML
            :param yaml.Loader constructor_read_loader: The loader to use for converting from YAML when reading from LUSID
            :param yaml.Loader constructor_update_loader: The loader to use for converting from YAML when writing to LUSID
            """

            # Maps the types of the element to the appropriate dumper method
            type_mapping = {
                "<class 'dict'>": "represent_mapping",
                "<class 'str'>": "represent_scalar",
                "<class 'list'>": "represent_sequence",
            }

            def representation_function_dumper(
                dumper: yaml.Dumper, data, repr: Callable = representation_function
            ) -> Callable:
                """
                This function is used by the dumper to turn a LUSID model object into YAML

                :param yaml.Dumper dumper: The dumper to use
                :param data: The data
                :param Callable repr: The base representation function

                :return: Callable
                """
                # Get the payload from the representation function
                payload = repr(data)
                # Get the appropriate dumper function to use
                representation_type = type_mapping[str((type(payload)))]

                # Construct and return the appropriate dumper representation function
                if representation_type == "represent_sequence":
                    return getattr(dumper, representation_type)(
                        tag, payload, flow_style=True
                    )
                else:
                    return getattr(dumper, representation_type)(tag, payload)

            # Add the dumper function
            yaml.add_representer(
                data_type=representation_model,
                representer=representation_function_dumper,
                Dumper=representation_dumper,
            )

            def constructor_function_read(
                loader: yaml.Loader, node, con: Callable = constructor_function
            ):
                """
                This function is used to construct a LUSID model from YAML when reading from LUSID

                :param yaml.Loader loader: The YAML loader to use
                :param node: The YAML node to construct the model from
                :param con: The base constructor function

                :return: lusid.models: The populated LUSID model
                """
                return constructor_read_model(*con(loader, node))

            def constructor_function_update(loader, node, con=constructor_function):
                """
                This function is used to construct a LUSID model from YAML when writing to LUSID. The only difference
                from the "constructor_function_read" is that it has a different model.

                :param yaml.Loader loader: The YAML loader to use
                :param node: The YAML node to construct the model from
                :param con: The base constructor function

                :return: lusid.models: The populated LUSID model
                """
                return constructor_update_model(*con(loader, node))

            # Add the YAML to LUSID model constructor for reading from LUSID
            yaml.add_constructor(
                tag=tag,
                constructor=constructor_function_read,
                Loader=constructor_read_loader,
            )

            # Add the YAML to LUSID model constructor for writing to LUSID
            yaml.add_constructor(
                tag=tag,
                constructor=constructor_function_update,
                Loader=constructor_update_loader,
            )

    @staticmethod
    def find_by_tag(node: yaml.Node, tag: str):
        """
        Utility function which searches the node until the specified tag is first found returning the discovered node

        :param yaml.Node node: The node to search
        :param str tag: The tag to search for in the node

        :return: yaml.Node: The first discovered node which matches the tag
        """
        for i in node.value:
            if i[0].value == tag:
                return i[1]

    @staticmethod
    def prepare_properties(properties_node_list: list) -> dict:
        """
        Utility which prepares a single property from a properties node so that it can be sent to LUSID as a dictionary

        :param list properties_node_list: The properties node to prepare

        :return dict(str, models.PerpetualProperty) properties: The property in a dictionary
        """
        if len(properties_node_list) > 0:
            single_property = {properties_node_list[0].key: properties_node_list[0]}
        else:
            single_property = {}

        return single_property

    # Set up the YAML converter for the TransactionSetConfigurationData (top level object)
    @staticmethod
    def config_rep(data):
        return {
            "transactionConfigRequests": data.transaction_configs,
            "sideConfigRequests": data.side_definitions,
        }

    @classmethod
    def config_con(cls, loader, node):
        txn = loader.construct_sequence(
            cls.find_by_tag(node, "transactionConfigRequests")
        )
        side = loader.construct_sequence(cls.find_by_tag(node, "sideConfigRequests"))
        return txn, side

    # Set up the YAML converter for the TransactionConfigurationData (transaction_configs)
    @staticmethod
    def trans_config_rep(data):
        return {
            "aliases": data.aliases,
            "movements": data.movements,
            "properties": list(data.properties.values()),
        }

    @classmethod
    def trans_config_con(cls, loader, node):
        a = loader.construct_sequence(cls.find_by_tag(node, "aliases"))
        m = loader.construct_sequence(cls.find_by_tag(node, "movements"))
        p = cls.prepare_properties(
            loader.construct_sequence(cls.find_by_tag(node, "properties"))
        )
        return a, m, p

    # Set up the YAML converter for the TransactionConfigurationMovementData (movements)
    @staticmethod
    def mvmt_rep(data):
        return [
            [abbrev(data.side), data.direction, abbrev(data.movement_types)],
            list(data.properties.values()),
            data.mappings,
        ]

    @classmethod
    def mvmt_con(cls, loader, node):
        s = loader.construct_sequence(node.value[0])
        properties = cls.prepare_properties(loader.construct_sequence(node.value[1]))
        side = unabbrev(s[0])
        direction = s[1]
        movement_types = unabbrev(s[2])
        mappings = loader.construct_sequence(node.value[2])

        return movement_types, side, direction, properties, mappings

    # Set up the YAML converter for the TransactionPropertyMapping (mappings)
    @staticmethod
    def pm_rep(data):
        if data.map_from is not None:
            return "{}=F:{}".format(data.property_key, data.map_from)
        else:
            return "{}=S:{}".format(data.property_key, data.set_to)

    @staticmethod
    def pm_con(loader, node):
        s = node.value.split("=")

        property_key = s[0]

        if s[1].startswith("F:"):
            map_from = s[1][2:]
            set_to = None
        else:
            set_to = s[1][2:]
            map_from = None

        return property_key, map_from, set_to

    # Set up the YAML converter for the TransactionConfigurationTypeAlias (aliases)
    @staticmethod
    def alias_rep(data):
        return [
            data.type,
            data.description,
            data.transaction_group,
            data.transaction_class,
            abbrev(data.transaction_roles),
        ]

    @staticmethod
    def alias_con(loader, node):
        s = loader.construct_sequence(node)
        return s[0], s[1], s[3], s[2], unabbrev(s[4])

    # Set up the YAML converter for the SideConfigurationDataRequest (side)
    @staticmethod
    def side_config_rep(data):
        return [
            data.side,
            data.security,
            data.currency,
            data.rate,
            data.units,
            data.amount,
        ]

    @staticmethod
    def side_config_con(loader, node):
        s = loader.construct_sequence(node)
        side = s[0]
        security = s[1]
        currency = s[2]
        rate = s[3]
        units = s[4]
        amount = s[5]
        return side, security, currency, rate, units, amount

    # Set up the YAML converter for PerpetualProperties (properties)
    @staticmethod
    def pp_rep(data):
        return abbrev("{}={}".format(data.key, data.value.label_value))

    @staticmethod
    def pp_con(loader, node):
        s = unabbrev(node.value).split("=")
        return (s[0], models.property_value.PropertyValue(s[1]))

    # END OF THE INITIALISER ############################################

    # Write the yaml file with validation that the custom view doesn't drop any
    # important data
    class TransactionSetConfigurationDataNoLinks:
        """
        This class is used to replace TransactionSetConfigurationData so that it has no extra information e.g. links
        which get converted to YAML.
        """

        def __init__(self, transaction_configs, side_definitions):
            self.transaction_configs = transaction_configs
            self.side_definitions = side_definitions

    def dump(self, obj, filename, raw=False):

        if raw:
            with open(filename, "w") as stream:
                yaml.dump(obj, stream)
        else:
            orig = yaml.dump(obj, width=500)
            cust = yaml.dump(obj, Dumper=CustomDumper, width=500)
            copy = yaml.dump(yaml.load(cust, Loader=QueryLoader), width=500)

            if orig != copy:
                print("DIFFS - writing to orig/copy")
                with open("orig", "w") as stream:
                    stream.write(orig)
                with open("copy", "w") as stream:
                    stream.write(copy)
            with open(filename, "w") as stream:
                stream.write(cust)

    # Get the YAML using the custom view
    def get_yaml(self, obj):
        return yaml.dump(obj, Dumper=CustomDumper)

    # Read a YAML file
    def load(self, filename):
        with open(filename, "r") as stream:
            return yaml.load(stream, Loader=QueryLoader)

    # Read a YAML file and convert to the UPDATE request message format
    def load_update(self, filename):
        with open(filename, "r") as stream:
            return yaml.load(stream, Loader=UpdateLoader)

    # Read a YAML string and convert to the UPDATE request message format
    def load_update_str(self, s):
        return yaml.load(s, Loader=UpdateLoader)
