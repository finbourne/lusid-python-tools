import unittest
from pathlib import Path
import pandas as pd
import lusid
from lusidtools import cocoon as cocoon
from parameterized import parameterized
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id


class CocoonTestsPortfolios(unittest.TestCase):
    api_factory = None

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @parameterized.expand(
        [
            [
                "Standard load",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                None,
                [],
            ],
            [
                "Add in some constants",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": "$AverageCost"},
                {},
                ["base_currency"],
                "operations001",
                None,
                [],
            ],
            [
                "Standard load with unique properties scope to ensure property creation",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id()}",
                None,
                [],
            ],
            [
                "Standard load with a single sub-holding-key",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                "operations001",
                ["Strategy"],
                [],
            ],
            [
                "Standard load with a multiple sub-holding-keys in a unique scope",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id()}",
                ["Strategy", "Broker"],
                [],
            ],
            [
                "Standard load with a sub-holding-key with scope specified",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id()}",
                ["Trader/Strategy"],
                [],
            ],
            [
                "Standard load with a sub-holding-key with domain and scope specified",
                "prime_broker_test",
                "data/metamorph_portfolios-unique.csv",
                {
                    "code": "FundCode",
                    "display_name": "display_name",
                    "created": "created",
                    "base_currency": "base_currency",
                },
                {"description": "description", "accounting_method": None},
                {},
                ["base_currency"],
                f"operations001_{create_scope_id()}",
                ["Transaction/Trader/Strategy"],
                [],
            ],
        ]
    )
    def test_load_from_data_frame_a_portfolios_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
        expected_outcome,
    ) -> None:
        """
        Test that portfolios can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list[str] property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param any expected_outcome: The expected outcome

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="portfolios",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys
        )

        self.assertEqual(
            first=len(responses["portfolios"]["success"]), second=len(data_frame)
        )

        response_codes = [
            response.id.code
            if isinstance(response, lusid.models.Portfolio)
            else response.origin_portfolio_id.code
            for response in responses["portfolios"]["success"]
        ]

        self.assertEqual(
            first=response_codes,
            second=list(data_frame[mapping_required["code"]].values),
        )
