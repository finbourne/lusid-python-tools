import logging
import time
import lusid_drive
import lusid
import unittest
import json
import os
from lusid_drive.rest import ApiException
from pprint import pprint
from lusidtools.cocoon.utilities import (
    checkargs,
)


@checkargs
def upload_file(
        files_api: lusid_drive.FilesApi,
        file_name: str,
        folder_path: str,
        body: str,

    ):
        
        try:
            x = files_api.create_file(
                x_lusid_drive_filename=file_name,
                x_lusid_drive_path=folder_path,
                content_length=len(body.encode('UTF-8')),
                body=body
                )
            logging.info(
                f"File created via the files API"
            )
            return x

        except lusid_drive.ApiException as e:
            detail = json.loads(e.body)
            if detail["code"] != 671:  # FileAlreadyExists
                raise e
            logging.exception(
                f"File already exists"
            )
            return detail

               
            
