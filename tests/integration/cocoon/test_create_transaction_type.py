import unittest
from pathlib import Path
import pandas as pd
import json
import lusid
import lusid.models as models
from lusidtools.cocoon.utilities import create_scope_id
import logging

logger = logging.getLogger()


class CocoonTestPortfolioGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        cls.portfolio_scope = create_scope_id()
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

    def log_error_requests_title(cls, domain, responses):
        if len(responses.get(domain, {}).get("errors", [])) > 0:
            for error in responses[domain]["errors"]:
                return logger.error(json.loads(error.body)["title"])


    def csv_to_data_frame_with_scope(cls, csv, scope):
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(csv))
        data_frame["Scope"] = scope
        return data_frame