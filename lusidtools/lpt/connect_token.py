import lusid


def connect(config):
    lusid_config = lusid.Configuration()
    lusid_config.access_token = config["token"]
    lusid_config.host = config["apiUrl"]

    return lusid.extensions.api_client.SyncApiClient(lusid_config), lusid
