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


def replace_current_transaction_alias(
    api_factory, updated_alias, movements, properties=None
):

    """
    This function overrides the current transaction types alias with a new configuration

    Parameters
    ----------
    api_factory: lusid.utilities.ClientApiFactory
        The LUSID api factory to use.
    updated_alias: lusid.models.TransactionConfigurationTypeAlias
        The alias which we want to update.
    movements: list[lusid.models.TransactionConfigurationMovementDataRequest]
        The movements to use for transaction type
    properties: dict[str, lusid.PerpetualProperty]
        The custom properties to assign to the transaction type

    Returns
    -------
    response : (lusid.models.TransactionSetConfigurationData)
        The response from setting the transaction type
    """

    # Call LUSID to get a list of the current transaction types

    current_transaction_types = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).list_configuration_transaction_types()

    transaction_configs_list = current_transaction_types.transaction_configs

    # Build the new transaction type you want to upload

    new_transaction_request = models.TransactionConfigurationDataRequest(
        aliases=[updated_alias], movements=movements, properties=properties,
    )

    # Delete the current configuration assigned to your replacement alias

    for txn_index, trans_type in enumerate(transaction_configs_list):

        find_new_alias = False

        for alias_index, alias in enumerate(trans_type.aliases):

            if (
                alias.type == updated_alias.type
                and alias.transaction_group == updated_alias.transaction_group
            ):

                del trans_type.aliases[alias_index]

                find_new_alias = True

                break

        else:
            continue

        # If there are no aliases assigned against the old movement, delete the transaction type configuration from LUSID
        # We don't want to leave unassigned movements in LUSID

        if len(trans_type.aliases) == 0:
            del transaction_configs_list[txn_index]

        break

    if find_new_alias is False:

        raise Warning(
            "The requested alias is not available in LUSID. No updates have been made."
        )

    transaction_configs_list.append(new_transaction_request)

    set_response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).set_configuration_transaction_types(
        transaction_set_configuration_data_request=models.TransactionSetConfigurationDataRequest(
            transaction_config_requests=transaction_configs_list,
            side_config_requests=current_transaction_types.side_definitions,
        )
    )

    return set_response
