import lusid
import lusid.models as models
import logging

logger = logging.getLogger()


def create_transaction_type_configuration(api_factory, alias, movements):
    """
    This function creates a transaction type configuration if it doesn't already exist.

    :param lusid.utilities.ClientApiFactory api_factory: The LUSID api factory to use
    :param lusid.models.TransactionConfigurationTypeAlias alias: An aliases with type and group
    :param list[lusid.models.TransactionConfigurationMovementDataRequest] movements: The movements to use for
    transaction type

    return (lusid.models.createtransactiontyperesponse) response: The response from creating the transaction type
    """

    # Call LUSID to get your transaction type configuration
    response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).list_configuration_transaction_types()

    aliases_current = [
        (alias.type, alias.transaction_group)
        for transaction_grouping in response.transaction_configs
        for alias in transaction_grouping.aliases
    ]

    logger.info(
        f"The LUSID enviornment currently has {len(aliases_current)} transaction aliases"
    )

    if (alias.type, alias.transaction_group) in aliases_current:
        logging.error(
            "The following alias already exists: "
            + f"Type of {alias.type} with source {alias.transaction_group}"
        )
        return response

    logger.info(f"Creating a new transaction aliases called: {alias.type}")

    response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).create_configuration_transaction_type(
        type=models.TransactionConfigurationDataRequest(
            aliases=[alias], movements=movements
        )
    )

    return response
