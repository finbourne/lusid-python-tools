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


def upsert_transaction_type_alias(api_factory, new_transaction_config):
    """
    This function overrides the current transaction types alias with a new configuration.
    The alias will be created if it does not already exist.

    Parameters
    ----------
    api_factory: lusid.utilities.ClientApiFactory
        The LUSID api factory to use.
    new_transaction_config: list[lusid.models.TransactionConfigurationDataRequest]
        A list of new transaction type configurations

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

    # Define function to delete current alias if it already exists

    def delete_current_alias(updated_alias):

        for txn_index, trans_type in enumerate(transaction_configs_list):

            for alias_index, alias in enumerate(trans_type.aliases):

                if (
                    alias.type == updated_alias.type
                    and alias.transaction_group == updated_alias.transaction_group
                ):
                    del trans_type.aliases[alias_index]

                    break

            else:
                continue

            # If there are no aliases assigned against the old movement, delete the transaction type configuration from LUSID
            # We don't want to leave unassigned movements in LUSID

            if len(trans_type.aliases) == 0:
                del transaction_configs_list[txn_index]

            break

    # Loop over list of new aliases and delete them if they already exist

    new_aliases_nested = [item.aliases for item in new_transaction_config]
    new_aliases = [item for sublist in new_aliases_nested for item in sublist]

    for item in new_aliases:
        delete_current_alias(item)

    # Then set the new aliases with new config

    transaction_configs_list = transaction_configs_list + new_transaction_config

    set_response = api_factory.build(
        lusid.api.SystemConfigurationApi
    ).set_configuration_transaction_types(
        transaction_set_configuration_data_request=models.TransactionSetConfigurationDataRequest(
            transaction_config_requests=transaction_configs_list,
            side_config_requests=current_transaction_types.side_definitions,
        )
    )

    return set_response
