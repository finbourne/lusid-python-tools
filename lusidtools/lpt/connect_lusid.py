import os
from urllib.request import quote

import lusid

from .refreshing_token import RefreshingToken


def connect(config, **kwargs):

    if "api" not in config.keys():
        config["api"] = {}
        config["api"]["tokenUrl"] = None
        config["api"]["username"] = None
        config["api"]["password"] = None
        config["api"]["clientId"] = None
        config["api"]["clientSecret"] = None
        config["api"]["apiUrl"] = None

    token_url = os.getenv("FBN_TOKEN_URL", config["api"]["tokenUrl"])
    username = os.getenv("FBN_USERNAME", config["api"]["username"])
    password = quote(os.getenv("FBN_PASSWORD", config["api"]["password"]), "*!")
    client_id = quote(os.getenv("FBN_CLIENT_ID", config["api"]["clientId"]), "*!")
    client_secret = quote(
        os.getenv("FBN_CLIENT_SECRET", config["api"]["clientSecret"]), "*!"
    )
    api_url = os.getenv("FBN_LUSID_API_URL", config["api"]["apiUrl"])

    token_request_body = (
        "grant_type=password&username={0}".format(username)
        + "&password={0}&scope=openid client groups".format(password)
        + "&client_id={0}&client_secret={1}".format(client_id, client_secret)
    )

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    config = lusid.Configuration()
    config.access_token = RefreshingToken(token_url, token_request_body, headers)
    config.host = api_url

    return (lusid.ApiClient(config), lusid)
