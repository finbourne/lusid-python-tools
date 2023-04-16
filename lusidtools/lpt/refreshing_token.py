import requests
from datetime import datetime
from datetime import timedelta
from collections import UserString

# Behaves like a string, but refreshes the
# OKTA credentials should they expire


class RefreshingToken(UserString):
    def __init__(self, token_url, token_request_body, headers):

        token_data = {"expires": datetime.now(), "credentials": ""}

        def get_token():
            if token_data["expires"] <= datetime.now():

                okta_response = requests.post(
                    token_url, data=token_request_body, headers=headers
                )
                if okta_response.status_code != 200:
                    print("OKTA authentication failed")
                    print(okta_response.text)
                    exit()

                d = dict(okta_response.json())
                token_data["expires"] = datetime.now() + timedelta(
                    seconds=d.get("expires_in", 3600) - 60
                )
                token_data["credentials"] = d["access_token"]

            return token_data["credentials"]

        self.token_fn = get_token

    # Call the token function to get the credentials
    # (refreshing them if necessary)
    # and then return the attribute for the resulting token
    def __getattribute__(self, name):
        token = object.__getattribute__(self, "token_fn")()
        if name == "data":
            return token
        return token.__getattribute__(name)
