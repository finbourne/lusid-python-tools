import yaml

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
fwd_abbrev = {v: k for k, v in rev_abbrev.items()}


# Methods to map between abbreviations
def abbrev(v):
    return fwd_abbrev.get(v, v)


def unabbrev(v):
    return rev_abbrev.get(v, v)


# Dumper class with the custom constructor/resolvers
class CustomDumper(yaml.Dumper):
    pass


# Loader class with the custom constructor for updates
# This is required because the input message uses different classes
class UpdateLoader(yaml.Loader):
    pass


class TxnConfigYaml:
    def __init__(self, models):
        # Initialisation will set up the custom constructor/resolvers
        # linked to the CustomDumper and UpdateLoader classes

        # Searches the node, to find the specified item
        def findByTag(node, tag):
            for i in node.value:
                if i[0].value == tag:
                    return i[1]

        # Property mapping representation
        def pm_rep(dumper, data):
            if data.map_from != None:
                s = "{}=F:{}".format(data.property_key, data.map_from)
            else:
                s = "{}=S:{}".format(data.property_key, data.set_to)
            return dumper.represent_scalar("!Map", s)

        yaml.add_representer(
            models.transaction_property_mapping.TransactionPropertyMapping,
            pm_rep,
            Dumper=CustomDumper,
        )

        def pm_con(loader, node):
            s = node.value.split("=")
            if s[1].startswith("F:"):
                map_from = s[1][2:]
                set_to = None
            else:
                set_to = s[1][2:]
                map_from = None

            return models.transaction_property_mapping.TransactionPropertyMapping(
                s[0], map_from, set_to
            )

        yaml.add_constructor("!Map", pm_con)

        # Constructor for the update message
        def upd_pm_con(loader, node):
            s = node.value.split("=")
            if s[1].startswith("F:"):
                map_from = s[1][2:]
                set_to = None
            else:
                set_to = s[1][2:]
                map_from = None

            return models.transaction_property_mapping_request.TransactionPropertyMappingRequest(
                s[0], map_from, set_to
            )

        yaml.add_constructor("!Map", upd_pm_con, Loader=UpdateLoader)

        # Perpetual property representation
        def pp_rep(dumper, data):
            return dumper.represent_scalar(
                "!Prop", abbrev("{}={}".format(data.key, data.value.label_value))
            )

        yaml.add_representer(
            models.perpetual_property.PerpetualProperty, pp_rep, Dumper=CustomDumper
        )

        def pp_con(loader, node):
            s = unabbrev(node.value).split("=")
            return (
                s[0],
                models.perpetual_property.PerpetualProperty(
                    s[0], models.property_value.PropertyValue(s[1])
                ),
            )

        yaml.add_constructor("!Prop", pp_con)
        yaml.add_constructor("!Prop", pp_con, Loader=UpdateLoader)

        # Movement types representation
        def mvmt_rep(dumper, data):
            return dumper.represent_sequence(
                "!Mvmt",
                [
                    [abbrev(data.side), data.direction, abbrev(data.movement_types)],
                    list(data.properties.values()),
                    data.mappings,
                ],
                True,
            )

        yaml.add_representer(
            models.transaction_configuration_movement_data.TransactionConfigurationMovementData,
            mvmt_rep,
            Dumper=CustomDumper,
        )

        def mvmt_con(loader, node):
            s = loader.construct_sequence(node.value[0])
            p = dict(loader.construct_sequence(node.value[1]))
            m = loader.construct_sequence(node.value[2])

            return models.transaction_configuration_movement_data.TransactionConfigurationMovementData(
                movement_types=unabbrev(s[2]),
                side=unabbrev(s[0]),
                direction=s[1],
                properties=p,
                mappings=m,
            )

        yaml.add_constructor("!Mvmt", mvmt_con)

        # Constructor for the update message
        def upd_mvmt_con(loader, node):
            s = loader.construct_sequence(node.value[0])
            p = dict(loader.construct_sequence(node.value[1]))
            m = loader.construct_sequence(node.value[2])

            return models.transaction_configuration_movement_data_request.TransactionConfigurationMovementDataRequest(
                movement_types=unabbrev(s[2]),
                side=unabbrev(s[0]),
                direction=s[1],
                properties=p,
                mappings=m,
            )

        yaml.add_constructor("!Mvmt", upd_mvmt_con, Loader=UpdateLoader)

        # Transaction alias representation
        def alias_rep(dumper, data):
            return dumper.represent_sequence(
                "!Alias",
                [
                    data.type,
                    data.description,
                    data.transaction_group,
                    data.transaction_class,
                    abbrev(data.transaction_roles),
                ],
            )

        yaml.add_representer(
            models.transaction_configuration_type_alias.TransactionConfigurationTypeAlias,
            alias_rep,
            Dumper=CustomDumper,
        )

        def alias_con(loader, node):
            s = loader.construct_sequence(node)
            return models.transaction_configuration_type_alias.TransactionConfigurationTypeAlias(
                s[0], s[1], s[3], s[2], unabbrev(s[4])
            )

        yaml.add_constructor("!Alias", alias_con)
        # Update message uses same class
        yaml.add_constructor("!Alias", alias_con, Loader=UpdateLoader)

        # Transaction configuration representation
        def config_rep(dumper, data):
            p = list(data.properties.values())
            return dumper.represent_mapping(
                "!Txn",
                {"aliases": data.aliases, "movements": data.movements, "properties": p},
            )

        yaml.add_representer(
            models.transaction_configuration_data.TransactionConfigurationData,
            config_rep,
            Dumper=CustomDumper,
        )

        def config_con(loader, node):
            a = loader.construct_sequence(findByTag(node, "aliases"))
            m = loader.construct_sequence(findByTag(node, "movements"))
            p = dict(loader.construct_sequence(findByTag(node, "properties")))
            return models.transaction_configuration_data.TransactionConfigurationData(
                a, m, p
            )

        yaml.add_constructor("!Txn", config_con)

        # Constructor for the update message
        def upd_config_con(loader, node):
            a = loader.construct_sequence(findByTag(node, "aliases"))
            m = loader.construct_sequence(findByTag(node, "movements"))
            p = dict(loader.construct_sequence(findByTag(node, "properties")))

            req = models.transaction_configuration_data_request.TransactionConfigurationDataRequest(
                a, m, p
            )
            return req

        yaml.add_constructor("!Txn", upd_config_con, Loader=UpdateLoader)

    # END OF THE INITIALISER ############################################

    # Write the yaml file with validation that the custom view doesn't drop any
    # important data
    def dump(self, obj, filename, raw=False):

        if raw:
            with open(filename, "w") as stream:
                yaml.dump(obj, stream)

        else:
            orig = yaml.dump(obj, width=500)
            cust = yaml.dump(obj, Dumper=CustomDumper, width=500)
            copy = yaml.dump(yaml.load(cust, Loader=UpdateLoader), width=500)

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
            return yaml.load(stream)

    # Read a YAML file and convert to the UPDATE request message format
    def load_update(self, filename):
        with open(filename, "r") as stream:
            return yaml.load(stream, Loader=UpdateLoader)

    # Read a YAML string and convert to the UPDATE request message format
    def load_update_str(self, s):
        return yaml.load(s, Loader=UpdateLoader)
