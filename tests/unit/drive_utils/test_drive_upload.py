import unittest
import os
import lusid_drive
from unittest import mock
from lusidtools.drive_utils import drive_utilities
from lusid_drive.rest import ApiException
from lusidtools import logger



class TestDriveFileUpload(unittest.TestCase):

    @classmethod
    def test_upload_file_positive(self) -> None:
        """
        Test that a file can be created successfully

        :return: None
        """
        api_client_mock = lusid_drive.FilesApi()
        api_client_mock.create_file = mock.MagicMock()

        with open('./lusidtools/drive_utils/data/TestDoc.rtf', mode="r") as file:
            body = file.read()

        x_lusid_drive_filename = 'TestDoc.rtf' # str | File name.
        x_lusid_drive_path = '/' # str | Drive path.
       
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

        with open('./lusidtools/drive_utils/data/TestDoc.rtf', mode="r") as file:
            body = file.read()

        x_lusid_drive_filename = 'TestDoc.rtf' # str | File name.
        x_lusid_drive_path = '/' # str | Drive path.
       
        drive_utilities.upload_file(api_client_mock,x_lusid_drive_filename, x_lusid_drive_path, body)
        api_client_mock.create_file.assert_called_once_with(x_lusid_drive_filename='TestDoc.rtf', x_lusid_drive_path='/', content_length=395, body=body)

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
        
        with open('./lusidtools/drive_utils/data/TestDoc.rtf', mode="r") as file:
            body = file.read()

        x_lusid_drive_filename = 'TestDoc.rtf' # str | File name.
        x_lusid_drive_path = '/' # str | Drive path.
        with self.assertRaises(ApiException):
            drive_utilities.upload_file(api_client_mock,x_lusid_drive_filename, x_lusid_drive_path, body)