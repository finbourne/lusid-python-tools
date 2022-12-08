import json

import lusid
import finbourne_access
import finbourne_access.utilities
import finbourne_identity

from finbourne_access import models as access_models
from finbourne_identity import models as identity_models

from lusidtools.cocoon.utilities import (
    checkargs,
)
import logging


@checkargs
def create_role(
    api_factory: lusid.utilities.ApiClientFactory,
    access_role_creation_request: access_models.RoleCreationRequest,
):
    """
    Creates a role through both the access and identity APIs

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory api_factory
        The api factory to use
    access_role_creation_request : access_models.RoleCreationRequest
        The role creation request to use

    Returns
    -------
    responses: None

    """

    api_client = api_factory.api_client
    lusid_api_url = api_client.configuration.host
    access_token = api_client.configuration.access_token

    # Build the access and identity API URLs from the LUSID API URL.
    access_api_url = lusid_api_url[: lusid_api_url.rfind("/") + 1] + "access"
    identity_api_url = lusid_api_url[: lusid_api_url.rfind("/") + 1] + "identity"

    # Build the access and identity API factories.
    access_api_factory = finbourne_access.utilities.ApiClientFactory(
        token=access_token,
        access_url=access_api_url,
    )
    identity_api_factory = finbourne_identity.utilities.ApiClientFactory(
        token=access_token,
        api_url=identity_api_url,
    )

    # Build the access and identity roles APIs.
    access_roles_api = access_api_factory.build(finbourne_access.RolesApi)
    identity_roles_api = identity_api_factory.build(finbourne_identity.RolesApi)

    # Create the role using the access API.
    try:
        access_roles_api.create_role(access_role_creation_request)
        logging.info(
            f"Role with code {access_role_creation_request.code} has been created via the access API"
        )
    except finbourne_access.ApiException as e:
        detail = json.loads(e.body)
        if detail["code"] not in [612, 613, 615]:  # RoleWithCodeAlreadyExists
            raise e
        logging.info(
            f"Role with code {access_role_creation_request.code} has already been created via the access API"
        )

    # Create the same role using the identity API.
    identity_role_creation_request = identity_models.CreateRoleRequest(
        access_role_creation_request.code
    )
    try:
        identity_roles_api.create_role(identity_role_creation_request)
        logging.info(
            f"Role with code {access_role_creation_request.code} has been created via the identity API"
        )
    except finbourne_identity.ApiException as e:
        detail = json.loads(e.body)
        if detail["code"] != 157:  # RoleWithCodeAlreadyExists
            raise e
        logging.info(
            f"Role with code {access_role_creation_request.code} has already been created via the identity API"
        )
