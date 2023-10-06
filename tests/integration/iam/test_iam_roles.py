import os
import unittest
from datetime import datetime, timedelta
from pathlib import Path
import pytz

import lusid
import finbourne_access
import finbourne_identity
from finbourne_access.utilities import ApiClientFactory as AccessApiClientFactory
from finbourne_access import models as access_models

from lusidtools import iam as iam
from lusidtools import logger


class IAMTestsRoles(unittest.TestCase):
    api_factory = None
    access_api_factory = None
    identity_api_factory = None

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.extensions.ApiClientFactory(
            config_loaders=(lusid.extensions.EnvironmentVariablesConfigurationLoader(), lusid.extensions.SecretsFileConfigurationLoader(secrets_file))
        )

        # Build the access and identity API factories
        cls.access_api_factory = finbourne_access.utilities.ApiClientFactory()
        cls.identity_api_factory = finbourne_identity.utilities.ApiClientFactory()

        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    def test_create_role_success(self) -> None:
        """
        Test that a role can be created successfully

        :return: None
        """

        role_code = "TestRoleCode"

        try:
            self.delete_role(role_code)
        except Exception as e:
            print(e)

        # Create a role creation request (using the access model)
        access_role_creation_request = create_access_role_creation_request(role_code)

        # Create a role using the LPT create_role method
        iam.roles.create_role(self.api_factory, access_role_creation_request)

        # Check that the role was correctly created through the access API
        access_role = self.get_access_role(role_code)
        self.assertEqual(first=access_role.id.code, second=role_code)

        # Check that the role was correctly created through the identity API
        identity_role = self.get_identity_role(role_code)
        self.assertEqual(first=identity_role.role_id.code, second=role_code)

    def delete_role(self, role_code):
        access_roles_api = self.access_api_factory.build(finbourne_access.RolesApi)
        identity_roles_api = self.identity_api_factory.build(
            finbourne_identity.RolesApi
        )

        # Try to delete the role through the access API
        try:
            access_roles_api.delete_role(role_code)
        except Exception as e:
            print(e)

        identity_role = self.get_identity_role(role_code)
        try:
            identity_roles_api.delete_role(identity_role.id)
        except Exception as e:
            print(e)

    def get_access_role(self, role_code):
        access_roles_api = self.access_api_factory.build(finbourne_access.RolesApi)

        return access_roles_api.get_role(role_code)

    def get_identity_role(self, role_code):
        # Build the identity API factory
        identity_roles_api = self.identity_api_factory.build(
            finbourne_identity.RolesApi
        )

        roles = identity_roles_api.list_roles()

        return next((r for r in roles if r.role_id.code == role_code))


def create_access_role_creation_request(role_code):
    return access_models.RoleCreationRequest(
        code=role_code,
        description="Test Role Description",
        resource=access_models.RoleResourceRequest(
            policy_id_role_resource=access_models.PolicyIdRoleResource(
                policies=[
                    access_models.PolicyId(scope="default", code="TestPolicyCode")
                ]
            )
        ),
        when=access_models.WhenSpec(
            activate=datetime.now(tz=pytz.utc) - timedelta(days=2),
            deactivate=datetime(9999, 12, 31, tzinfo=pytz.utc),
        ),
    )
