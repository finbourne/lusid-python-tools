import urllib.request
import unittest
import requests
from requests.auth import HTTPProxyAuth
import urllib

class ConnectivityTest(unittest.TestCase):
    def verify_connection(self, url):
        connection_status = urllib.request.urlopen(url).getcode()
        self.assertEqual(connection_status, 200, msg="FAILED: " + url)


    def test_https(self):
        self.verify_connection("https://www.howsmyssl.com/a/check")

    def test_Lusid(self):
        self.verify_connection("https://myco.lusid.com/api/api/metadata/versions")

    def test_okta(self):
        self.verify_connection("https://lusid-thomasdotred.okta.com/oauth2/aus4rl9zppr4CJYe32p7/.well-known/oauth"
                               "-authorization-server")

    def verify_connection_with_proxy(self, url):

        proxy_address = "192.168.1.1:8888"  # enter proxy address and port number here
        username = "email@demo.com"                # enter Username here
        password = r"PASSWORD"                # enter password here
        auth = HTTPProxyAuth(username, password)
        prox = {"https": proxy_address}

        r = requests.get(url, proxies=prox, auth=auth)
        self.assertEqual(r.status_code, 200, msg="FAILED: " + url)

    @unittest.skip("Proxy not configured")
    def test_https_with_proxy(self):
        self.verify_connection_with_proxy("https://www.howsmyssl.com/a/check")

    @unittest.skip("Proxy not configured")
    def test_Lusid_with_proxy(self):
        self.verify_connection_with_proxy("https://myco.lusid.com/api/api/metadata/versions")

    @unittest.skip("Proxy not configured")
    def test_okta_with_proxy(self):
        self.verify_connection_with_proxy(
            "https://lusid-thomasdotred.okta.com/oauth2/aus4rl9zppr4CJYe32p7/.well-known/oauth"
            "-authorization-server")
