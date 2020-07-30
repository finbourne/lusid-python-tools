import unittest
from pathlib import Path
import lusid
import lusid.models as models
from lusidtools.cocoon.utilities import create_scope_id
from lusidtools.cocoon.transaction_type_upload import replace_current_transaction_alias
import logging

logger = logging.getLogger()
new_transaction_type = create_scope_id().replace("-", "")


class CocoonTestTransactionTypeUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.system_configuration_api = cls.api_factory.build(
            lusid.api.SystemConfigurationApi
        )

        cls.alias = [
            models.TransactionConfigurationTypeAlias(
                type=new_transaction_type,
                description="TESTBUY1",
                transaction_class="TESTBUY1",
                transaction_group="SYSTEM1",
                transaction_roles="AllRoles",
            )
        ]

        cls.movements = [
            models.TransactionConfigurationMovementData(
                movement_types="StockMovement",
                side="Side1",
                direction=1,
                properties={},
                mappings=[],
            ),
            models.TransactionConfigurationMovementData(
                movement_types="CashCommitment",
                side="Side2",
                direction=1,
                properties={},
                mappings=[],
            ),
        ]

        cls.create_transaction_response = cls.system_configuration_api.create_configuration_transaction_type(
            transaction_configuration_data_request=models.TransactionConfigurationDataRequest(
                aliases=cls.alias, movements=cls.movements
            )
        )

    def test_update_current_alias_with_new_movements(self):

        updated_movements = [
            models.TransactionConfigurationMovementData(
                movement_types="StockMovement",
                side="Side1",
                direction=1,
                properties={},
                mappings=[],
            ),
            models.TransactionConfigurationMovementData(
                movement_types="CashCommitment",
                side="Side2",
                direction=1,
                properties={},
                mappings=[],
            ),
        ]

        replace_current_transaction_alias(
            api_factory=self.api_factory,
            updated_alias=self.alias[0],
            movements=updated_movements,
        )

        get_transaction_types = (
            self.system_configuration_api.list_configuration_transaction_types()
        )

        uploaded_alias = []

        for trans_type in get_transaction_types.transaction_configs:
            for alias in trans_type.aliases:
                if alias.type == new_transaction_type:
                    uploaded_alias.append(trans_type)

        alias_for_test = models.TransactionConfigurationData(
            aliases=self.alias, movements=updated_movements, properties={}
        )

        self.assertEqual(uploaded_alias[0], alias_for_test)

    def test_update_alias_which_does_not_exist(self):

        updated_movements = [
            models.TransactionConfigurationMovementData(
                movement_types="StockMovement",
                side="Side1",
                direction=-1,
                properties={},
                mappings=[],
            ),
            models.TransactionConfigurationMovementData(
                movement_types="CashCommitment",
                side="Side2",
                direction=1,
                properties={},
                mappings=[],
            ),
        ]

        updated_alias = models.TransactionConfigurationTypeAlias(
            type=create_scope_id().replace("-", ""),
            description="TESTBUY1",
            transaction_class="TESTBUY1",
            transaction_group="SYSTEM1",
            transaction_roles="AllRoles",
        )

        with self.assertRaises(Warning) as error:
            replace_current_transaction_alias(
                api_factory=self.api_factory,
                updated_alias=updated_alias,
                movements=updated_movements,
            )

        self.assertEqual(
            error.exception.args[0],
            "The requested alias is not available in LUSID. No updates have been made.",
        )
