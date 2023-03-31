import unittest
import os
import lusid
import lusid_drive
from pathlib import Path
from unittest import mock
from unittest.mock import patch
from lusidtools.drive_utils import drive_utilities as drive_utilities
from lusid_drive.rest import ApiException
from lusidtools import logger
from pprint import pprint
from tests.unit.cocoon.mock_api_factory import MockApiFactory



class TestDriveFileUpload(unittest.TestCase):
    api_factory = None
    access_api_factory = None
    identity_api_factory = None

    @classmethod
    def setUpClass(cls) -> None:

        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
           api_secrets_filename=secrets_file
         )
        
        api_client_mock = mock.MagicMock()
        cls.api_factory.api_client = mock.MagicMock(return_value= api_client_mock)
        cls.api_client_mock = lusid_drive.FilesApi()
        cls.api_client_mock.create_file = mock.MagicMock()

        # Get the LUSID API URL and the access token from the current api_factory
        api_client = cls.api_factory.api_client
        lusid_api_url = api_client.configuration.host
        access_token = api_client.configuration.access_token

        # Build the drive API URL from the LUSID API URL
        drive_api_url = lusid_api_url[: lusid_api_url.rfind("/") + 1] + "drive"

        configuration = lusid_drive.Configuration(
        host=drive_api_url
        )
        configuration.access_token = access_token

        # Build the files API factories
        with lusid_drive.ApiClient(configuration) as api_client:
            files_api = lusid_drive.FilesApi(api_client)

        cls.logger = logger.LusidLogger(os.getenv("FBN_LOG_LEVEL", "info"))


    def test_upload_file_positive(self) -> None:
        """
        Test that a file can be created successfully

        :return: None
        """
        api_client_mock = lusid_drive.FilesApi()
        api_client_mock.create_file = mock.MagicMock()

        file = open('./lusidtools/drive_utils/data/TestDoc.rtf').read()

        x_lusid_drive_filename = 'TestDoc.rtf' # str | File name.
        x_lusid_drive_path = '/' # str | Drive path.
        body = file # str | File


        drive_utilities.upload_file(api_client_mock,x_lusid_drive_filename, x_lusid_drive_path, body)
        api_client_mock.create_file.assert_called_once_with(x_lusid_drive_filename='TestDoc.rtf', x_lusid_drive_path='/', content_length=395, body=body)

    def test_upload_file_already_exists(self) -> None:
        """
        Test uploading a file when it already exists

        :return: None
        """
        api_client_mock = lusid_drive.FilesApi()

        http_resp = mock.MagicMock()
        http_resp.data = '{"code": 671}'

        api_client_mock.create_file = mock.MagicMock()
        api_client_mock.create_file.side_effect = lusid_drive.ApiException(http_resp= http_resp)

        file = open('./lusidtools/drive_utils/data/TestDoc.rtf').read()

        x_lusid_drive_filename = 'TestDoc.rtf' # str | File name.
        x_lusid_drive_path = '/' # str | Drive path.
        body = file # str | File

        
        drive_utilities.upload_file(api_client_mock,x_lusid_drive_filename, x_lusid_drive_path, body)
        api_client_mock.create_file.assert_called_once_with( x_lusid_drive_filename='TestDoc.rtf', x_lusid_drive_path='/', content_length=395, body=body)

    def test_upload_file_exception(self) -> None:
        """
        Test uploading a file which raises any other exception other than file_already_exists

        :return: None
        """
        api_client_mock = lusid_drive.FilesApi()
        http_resp = mock.MagicMock()

        #Testing any code other than 671 (File already exists)
        http_resp.data = '{"code": 111}'
        api_client_mock.create_file = mock.MagicMock()

        api_client_mock.create_file.side_effect = lusid_drive.ApiException(http_resp= http_resp)

        file = open('./lusidtools/drive_utils/data/TestDoc.rtf').read()

        x_lusid_drive_filename = 'TestDoc.rtf' # str | File name.
        x_lusid_drive_path = '/' # str | Drive path.
        body = file # str | File


        with self.assertRaises(ApiException):
            drive_utilities.upload_file(api_client_mock,x_lusid_drive_filename, x_lusid_drive_path, body)