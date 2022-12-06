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

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        print(secrets_file)
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))

    def test_create_role_success(
        self
    ) -> None:
        """
        Test that a role can be created successfully

        :return: None
        """

        role_code = "TestRoleCode"
        access_role_creation_request = access_models.RoleCreationRequest(
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
                deactivate=datetime(9999, 12, 31, tzinfo=pytz.utc)
            )
        )

        # Create a role using the LPT create_role method
        iam.roles.create_role(
            self.api_factory,
            access_role_creation_request
        )

        # Get the LUSID API URL and the access token from the current api_factory
        api_client = self.api_factory.api_client
        lusid_api_url = api_client.configuration.host
        access_token = api_client.configuration.access_token

        # Build the access and identity API URLs from the LUSID API URL.
        access_api_url = lusid_api_url[: lusid_api_url.rfind("/") + 1] + "access"
        identity_api_url = lusid_api_url[: lusid_api_url.rfind("/") + 1] + "identity"

        # Build the access and identity API factories.
        access_api_factory = finbourne_access.utilities.ApiClientFactory(
            token=access_token,
            access_url=access_api_url
        )
        identity_api_factory = finbourne_identity.utilities.ApiClientFactory(
            token=api_client.configuration.access_token,
            api_url=identity_api_url,
        )

        access_roles_api = access_api_factory.build(finbourne_access.RolesApi)
        identity_roles_api = identity_api_factory.build(finbourne_identity.RolesApi)

        access_role = access_roles_api.get_role(role_code)
        self.assertEqual(
            first=access_role.id.code,
            second=role_code
        )

        identity_roles = identity_roles_api.list_roles()
        identity_roles_codes = [role.role_id.code for role in identity_roles]
        self.assertIn(
            member=role_code,
            container=identity_roles_codes
        )
