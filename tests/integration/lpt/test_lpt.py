import os
import unittest
from pathlib import Path
from unittest import mock

from pandas import DataFrame

from lusidtools.lpt import (
    lpt,
    lse,
    qry_holdings as qh,
    qry_aggregate_holdings as qah,
    upload_portfolio as up,
    create_instr as ci,
    create_properties as cp,
    create_orders as co,
    qry_reconcile_holdings as qrh,
)


class LptTests(unittest.TestCase):
    api = None
    work_dir = None

    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        personal_access_token = os.getenv("FBN_ACCESS_TOKEN", None)
        if personal_access_token is None:
            cls.api = lse.connect(secrets=secrets_file, stats="-")  # stats to stdout
        else:
            cls.api = lse.connect(env=["token"], apiUrl=os.getenv("FBN_LUSID_API_URL"), token=personal_access_token, stats="-")  # stats to stdout

        # delete the properties that are created in the tests
        properties = [
            "Transaction/JLH/sub-acct",
            "Transaction/JLH/account",
            "Holding/JLH/prop1",
            "Holding/JLH/prop2",
        ]

        for p in properties:
            key_parts = p.split("/")

            if len(key_parts) != 3:
                raise Exception(f"invalid property key: {p}")

            try:
                cls.api.call.delete_property_definition(
                    domain=key_parts[0], scope=key_parts[1], code=key_parts[2]
                )
            except:
                pass

        cls.work_dir = os.getcwd()
        cls.test_data_path = Path(__file__).parent
        os.chdir(cls.test_data_path)

    @classmethod
    def tearDownClass(cls):
        cls.api.dump_stats()
        os.chdir(cls.work_dir)

    def target_portfolios_exist(cls):
        return (
            cls.api.call.get_portfolio(scope="JLH", code="JLH1").is_right()
            and cls.api.call.get_portfolio(scope="JLH", code="JLH2").is_right()
            and cls.api.call.get_portfolio(scope="JLH", code="JLH3").is_right()
            and cls.api.call.get_portfolio(
                scope="Finbourne-Examples-lusid-tools", code="Global-Equity"
            ).is_right()
        )

    def test_connect_with_token(self):
        api = lse.connect(
            env=["token"],
            token="test_access_token",
            secrets="none",
            stats="-",
            apiUrl="lusid.url",
        )
        self.assertIsNotNone(api)

    @mock.patch.dict(os.environ, {"FBN_LUSID_API_URL": ""})
    def test_connect_with_missing_secrets_file(self):
        with self.assertRaises(Exception) as context:
            lse.connect(
                env=["lusid"],
                token="test_access_token",
                secrets="i-do-not-exist.json",
                stats="-",
                apiUrl="lusid.url",
            )
        self.assertTrue("Missing the following config" in str(context.exception))

    def test_load_instruments(self):

        # Load instruments from the file: examples/ibm-msft.csv
        ci.process_args(
            self.api,
            ci.parse(args=[f".{os.path.sep}examples{os.path.sep}ibm-msft.csv"]),
        ).if_left(lambda r: self.fail(r))

    def test_upsert_orders(self):
        # Load orders from the file: examples/orders.csv
        co.process_args(
            self.api,
            co.parse(
                args=[
                    "--identifiers",
                    "Ticker",
                    "--",
                    f".{os.path.sep}examples{os.path.sep}orders.csv",
                ]
            ),
        ).if_left(lambda r: self.fail(r))

    def test_create_properties(self):

        # Create Properties
        result = cp.process_args(
            self.api,
            cp.parse(
                args=[
                    "--prop",
                    "Transaction/JLH/sub-acct",
                    "Transaction/JLH/account",
                    "Holding/JLH/prop1",
                    "Holding/JLH/prop2",
                ]
            ),
        )

        if result is not None:
            result.if_left(lambda left: self.fail(lpt.display_error(left)))

    def test_upload_holding(self):

        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        up.process_args(
            self.api,
            up.parse(
                args=[
                    "JLH",
                    "JLH1",
                    "-p",
                    f"{self.test_data_path.joinpath('examples').joinpath('holdings-examples.xlsx')}",
                    "2019-01-01",
                ]
            ),
        ).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertEqual(right, "Done!"),
        )

    def test_upload_multiple_holdings(self):

        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        # Load JLH2 and JLH3 Portfolios from second sheet from example Excel file
        up.process_args(
            self.api,
            up.parse(
                args=[
                    "JLH",
                    "col:portfolio-code",  # col: prefix indicates code is in the sheet
                    "-p",
                    f"{self.test_data_path.joinpath('examples').joinpath('holdings-examples.xlsx:Multiple')}",
                    "2019-01-01",
                ]
            ),
        ).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertEqual(right, "Done!"),
        )

    def test_upload_transactions(self):

        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        # Load JLH1 Transactions from first sheet from example Excel file
        up.process_args(
            self.api,
            up.parse(
                args=[
                    "JLH",
                    "JLH1",
                    "-t",
                    f"{self.test_data_path.joinpath('examples').joinpath('transactions-examples.xlsx')}",
                ]
            ),
        ).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertEqual(right, "Done!"),
        )

    def test_upload_multiple_transaction(self):

        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        # Load JLH2 and JLH3 Transactions from second sheet from example Excel file
        up.process_args(
            self.api,
            up.parse(
                args=[
                    "JLH",
                    "col:portfolio-code",  # col: prefix indicates code is in the sheet
                    "-t",
                    f"{self.test_data_path.joinpath('examples').joinpath('transactions-examples.xlsx:Multiple')}",
                ]
            ),
        ).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertEqual(right, "Done!"),
        )

    def test_get_holdings(self):

        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        # Query Portfolio JLH3
        qh.process_args(self.api, qh.parse(args=["JLH", "JLH3", "-t"])).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertIsInstance(right, DataFrame),
        )

    def test_aggregate_holdings(self):

        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        qah.process_args(
            self.api,
            qah.parse(
                args=[
                    "Finbourne-Examples-lusid-tools",
                    "Global-Equity",
                    "2020-01-01",
                    "--pricing-scope",
                    "Finbourne-Examples-lusid-tools",
                    "--recipe",
                    "FinbourneExamplesRecipeMidThenBid",
                    "--properties",
                    "Instrument/JLH/AssetClass",
                    "Instrument/JLH/Sector",
                    "Instrument/JLH/AssetType",
                ]
            ),
        ).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertIsInstance(right, DataFrame, f"{right} is not of type DataFrame"),
        )

    def test_reconcile_holdings(self):
        if not self.target_portfolios_exist():
            self.skipTest("missing target portfolios")

        qrh.process_args(
            self.api,
            qrh.parse(args=["JLH", "JLH1", "2020-01-01", "JLH", "JLH3", "2020-01-01"]),
        ).match(
            lambda left: self.fail(lpt.display_error(left)),
            lambda right: self.assertIsInstance(right, DataFrame),
        )
