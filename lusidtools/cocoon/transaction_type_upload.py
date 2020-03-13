import lusid
import lusid.models as models
import logging

def create_transaction_type_configuration(api_factory, new_alias, movements):
    """
    This function creates a transaction type configuration if it doesn't already exist.

    param (lusid.utilities.ClientApiFactory) api_factory: The LUSID api factory to use
    param (lusid.models.TransactionConfigurationTypeAlias) new_alias: A new alias object with a type and group.
    param (list[lusid.models.TransactionConfigurationMovementDataRequest]) movements: The movements to use for the transaction type.

    return (lusid.models.createtransactiontyperesponse) response: The response from creating the transaction type
    """

    # Call LUSID to get your transaction type configuration
    response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).list_configuration_transaction_types()

    current_aliases = [
        (alias.type, alias.transaction_group)
        for transaction_grouping in response.transaction_configs
        for alias in transaction_grouping.aliases
    ]

    logging.info(
        f"The LUSID enviornment currently has {len(current_aliases)} transaction aliases"
    )

    if (new_alias.type, new_alias.transaction_group) in current_aliases:
        logging.info(
            "The following alias already exists: "
            + f"Type of {new_alias.type} with source {new_alias.transaction_group}"
        )
        return response

    logging.info(f"Creating a new transaction aliases called: {alias_new.type}")

    response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).create_configuration_transaction_type(
        type=models.TransactionConfigurationDataRequest(
            aliases=[new_alias], movements=movements, properties=None
        )
    )

    return response
