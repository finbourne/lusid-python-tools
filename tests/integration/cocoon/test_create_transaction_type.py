import unittest
from pathlib import Path
import lusid
import lusid.models as models
from lusidtools.cocoon.utilities import create_scope_id
from lusidtools.cocoon.transaction_type_upload import (
    create_transaction_type_configuration,
)
import logging

logger = logging.getLogger()


class CocoonTestTransactionTypeUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        cls.alias = models.TransactionConfigurationTypeAlias(
            type=create_scope_id().replace("-", ""),
            description="TESTBUY1",
            transaction_class="TESTBUY1",
            transaction_group="SYSTEM1",
            transaction_roles="AllRoles",
        )

        cls.movements = [
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
                direction=-1,
                properties={},
                mappings=[],
            ),
        ]

        cls.response = create_transaction_type_configuration(
            cls.api_factory, cls.alias, cls.movements
        )

    def test_create_new_txn_type(self):

        """
        Create a new transaction type.
        Verify that the new transaction type is created.
        """

        self.assertEqual(
            set(self.response.transaction_configs[-1].aliases[0].to_dict().items()),
            set(self.alias.to_dict().items()),
        )

        self.assertEqual(
            sorted(
                [
                    item.to_dict()
                    for item in self.response.transaction_configs[-1].movements
                ],
                key=lambda item: item.get("movement_types"),
            ),
            sorted(
                [item.to_dict() for item in self.movements],
                key=lambda item: item.get("movement_types"),
            ),
        )

    def test_create_duplication_txn_type_throws_error(self):

        """
        Attempt to create a transaction type which already exists.
        Verify that function returns the correct log message.
        """

        with self.assertLogs() as context_manager:

            create_transaction_type_configuration(
                self.api_factory, self.alias, self.movements
            )

            self.assertEqual(
                f"ERROR:root:The following alias already exists: Type of {self.alias.type} with source {self.alias.transaction_group}",
                context_manager.output[1],
            )
