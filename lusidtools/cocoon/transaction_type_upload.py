import lusid
import lusid.models as models
import logging

logger = logging.getLogger()


def create_transaction_type_configuration(api_factory, alias, movements):
    """
    This function creates a transaction type configuration if it doesn't already exist.

    Parameters
    ----------
    api_factory : lusid.utilities.ClientApiFactory
        The LUSID api factory to use
    alias : lusid.models.TransactionConfigurationTypeAlias
        An aliases with type and group
    movements : list[lusid.models.TransactionConfigurationMovementDataRequest]
        The movements to use for
        transaction type

    Returns
    -------
    response : (lusid.models.createtransactiontyperesponse)
        The response from creating the transaction type
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
        logging.warning(
            "The following alias already exists: "
            + f"Type of {alias.type} with source {alias.transaction_group}"
        )
        return response

    logger.info(f"Creating a new transaction aliases called: {alias.type}")

    response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).create_configuration_transaction_type(
        transaction_configuration_data_request=models.TransactionConfigurationDataRequest(
            aliases=[alias], movements=movements
        )
    )

    return response
