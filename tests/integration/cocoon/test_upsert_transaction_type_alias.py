import unittest
from pathlib import Path
import lusid
import lusid.models as models
from lusidtools.cocoon.utilities import create_scope_id
from lusidtools.cocoon.transaction_type_upload import upsert_transaction_type_alias
import logging
import json

logger = logging.getLogger()
new_buy_transaction_type = "BUY-LPT-TEST"
new_sell_transaction_type = "SELL-LPT-TEST"
type_which_does_not_exist = create_scope_id()


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

        cls.class_transaction_type_config = [
            models.TransactionConfigurationDataRequest(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type=new_buy_transaction_type,
                        description="TESTBUY1",
                        transaction_class="TESTBUY1",
                        transaction_group="SYSTEM1",
                        transaction_roles="AllRoles",
                    )
                ],
                movements=[
                    models.TransactionConfigurationMovementDataRequest(
                        movement_types="StockMovement",
                        side="Side1",
                        direction=1,
                        properties={},
                        mappings=[],
                    ),
                    models.TransactionConfigurationMovementDataRequest(
                        movement_types="CashCommitment",
                        side="Side2",
                        direction=1,
                        properties={},
                        mappings=[],
                    ),
                ],
                properties={},
            ),
            models.TransactionConfigurationDataRequest(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type=new_sell_transaction_type,
                        description="TESTSELL1",
                        transaction_class="TESTSELL1",
                        transaction_group="SYSTEM1",
                        transaction_roles="AllRoles",
                    )
                ],
                movements=[
                    models.TransactionConfigurationMovementDataRequest(
                        movement_types="StockMovement",
                        side="Side1",
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                    models.TransactionConfigurationMovementDataRequest(
                        movement_types="CashCommitment",
                        side="Side2",
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                ],
                properties={},
            ),
        ]

        for trans_type in cls.class_transaction_type_config:

            try:

                cls.create_transaction_response = cls.system_configuration_api.create_configuration_transaction_type(
                    transaction_configuration_data_request=trans_type
                )

            except lusid.exceptions.ApiException as e:
                if json.loads(e.body)["code"] == 231:
                    logger.info(json.loads(e.body)["title"])

    def test_update_current_alias_with_new_movements(self):

        new_movements_alias = [
            models.TransactionConfigurationData(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type=new_buy_transaction_type,
                        description="TESTBUY1",
                        transaction_class="TESTBUY1",
                        transaction_group="SYSTEM1",
                        transaction_roles="AllRoles",
                    )
                ],
                movements=[
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
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                ],
                properties={},
            )
        ]

        upsert_transaction_type_alias(self.api_factory, new_movements_alias)

        get_transaction_types = (
            self.system_configuration_api.list_configuration_transaction_types()
        )

        uploaded_alias = []

        for trans_type in get_transaction_types.transaction_configs:
            for alias in trans_type.aliases:
                if alias.type == new_movements_alias[0].aliases[0].type:
                    uploaded_alias.append(trans_type)

        self.assertEqual(uploaded_alias[0], new_movements_alias[0])

    def test_update_alias_which_does_not_exist(self):

        alias_which_does_not_exist = [
            models.TransactionConfigurationData(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type=new_buy_transaction_type,
                        description="TESTBUY1",
                        transaction_class="TESTBUY1",
                        transaction_group="SYSTEM1",
                        transaction_roles="AllRoles",
                    )
                ],
                movements=[
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
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                ],
                properties={},
            )
        ]

        upsert_transaction_type_alias(self.api_factory, alias_which_does_not_exist)

        get_transaction_types = (
            self.system_configuration_api.list_configuration_transaction_types()
        )

        uploaded_alias = []

        for trans_type in get_transaction_types.transaction_configs:
            for alias in trans_type.aliases:
                if alias.type == alias_which_does_not_exist[0].aliases[0].type:
                    uploaded_alias.append(trans_type)

        self.assertEqual(uploaded_alias[0], alias_which_does_not_exist[0])

    def test_update_multiple_current_alias_with_new_movements(self):

        trans_type_with_multiple_alias = [
            models.TransactionConfigurationData(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type=new_buy_transaction_type,
                        description="TESTBUY1",
                        transaction_class="TESTBUY1",
                        transaction_group="SYSTEM1",
                        transaction_roles="AllRoles",
                    )
                ],
                movements=[
                    models.TransactionConfigurationMovementData(
                        movement_types="StockMovement",
                        side="Side2",
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                    models.TransactionConfigurationMovementData(
                        movement_types="CashCommitment",
                        side="Side1",
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                ],
                properties={},
            ),
            models.TransactionConfigurationData(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type=new_sell_transaction_type,
                        description="TESTSELL1",
                        transaction_class="TESTSELL1",
                        transaction_group="SYSTEM1",
                        transaction_roles="AllRoles",
                    )
                ],
                movements=[
                    models.TransactionConfigurationMovementData(
                        movement_types="StockMovement",
                        side="Side2",
                        direction=-1,
                        properties={},
                        mappings=[],
                    ),
                    models.TransactionConfigurationMovementData(
                        movement_types="StockMovement",
                        side="Side2",
                        direction=1,
                        properties={},
                        mappings=[],
                    ),
                ],
                properties={},
            ),
        ]

        upsert_transaction_type_alias(self.api_factory, trans_type_with_multiple_alias)

        get_transaction_types = (
            self.system_configuration_api.list_configuration_transaction_types()
        )

        uploaded_alias = []

        for trans_type in get_transaction_types.transaction_configs:
            for alias in trans_type.aliases:
                if alias.type in [new_buy_transaction_type, new_sell_transaction_type]:
                    uploaded_alias.append(trans_type)

        self.assertEqual(uploaded_alias, trans_type_with_multiple_alias)
