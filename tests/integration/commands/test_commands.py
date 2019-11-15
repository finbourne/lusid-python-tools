# Import setup modules
import unittest
import uuid

import pandas as pd
from pathlib import Path

# Import modules for testing
import lusidtools.lpt.qry_scopes as scopes
import lusidtools.lpt.qry_aggregate_holdings as agg
import lusidtools.lpt.qry_portfolios as portfolios
import lusidtools.lpt.get_instruments as get_instruments
import lusidtools.lpt.qry_portfolio_groups as list_portfolio_group
import lusidtools.lpt.qry_portfolio_properties as get_port_props
import lusidtools.lpt.qry_properties as get_props
import lusidtools.lpt.qry_constituents as get_port_cons
import lusidtools.lpt.qry_holdings as get_port_holdings
import lusidtools.lpt.qry_instr_ids as get_instr_id_types
import lusidtools.lpt.qry_instruments as list_instruments
import lusidtools.lpt.qry_portfolio_commands as get_port_commands
import lusidtools.lpt.qry_target_holdings as get_holdings_adj
import lusidtools.lpt.qry_transactions as get_transactions
import lusidtools.lpt.search_instruments as search_instruments
import lusidtools.lpt.create_group_portfolios as cgp
import lusidtools.lpt.upload_portfolio as up
import lusidtools.lpt.upload_quotes as uq


class CommandsTests(unittest.TestCase):
    test_scope = "lpt-test-scope"
    test_portfolio = "test_port"
    test_date = "2019-10-10"

    @classmethod
    def setUpClass(cls) -> None:
        cls.secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")

    def display_df(self, df, decimals=2):
        fmt = "{:,." + str(decimals) + "f}"
        pd.options.display.float_format = fmt.format
        pd.set_option("max_colwidth", -1)

        try:
            if len(df) == 1 and len(df.columns) > 5:
                df = df.T
            with pd.option_context("display.width", None, "display.max_rows", 1000):
                return df.fillna("")
        except:
            return df

    def validate_results_df(self, df):
        pass
        # self.assertIsInstance(df, pd.DataFrame)

    def validate_results_dict(self, response):
        self.assertIsInstance(response, dict)
        # self.assertTrue(len(df) > 1)

    def validate_success_string(self, response):
        self.assertTrue(response, "Done!")

    def test_scopes(self):

        result = scopes.main(
            parse=lambda: scopes.parse(args=["--secrets", f"{self.secrets_file}"]),
            display_df=self.display_df,
        )

        self.validate_results_df(result)

    def test_agg(self):

        result = agg.main(
            parse=lambda: agg.parse(
                args=[
                    self.test_scope,
                    self.test_portfolio,
                    self.test_date,
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )

        self.validate_results_df(result)

    def test_portfolios(self):

        result = portfolios.main(
            parse=lambda: portfolios.parse(
                args=[self.test_scope, "--secrets", f"{self.secrets_file}"]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_instr(self):

        result = get_instruments.main(
            parse=lambda: get_instruments.parse(
                args=[self.test_scope, "--secrets", f"{self.secrets_file}"]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_list_portfolio_group(self):

        result = list_portfolio_group.main(
            parse=lambda: list_portfolio_group.parse(
                args=[self.test_scope, "--secrets", f"{self.secrets_file}"]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_get_port_props(self):

        result = get_port_props.main(
            parse=lambda: get_port_props.parse(
                args=[
                    self.test_scope,
                    self.test_portfolio,
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_get_props(self):

        result = get_props.main(
            parse=lambda: get_props.parse(args=["--secrets", f"{self.secrets_file}"]),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_get_port_cons(self):

        result = get_port_cons.main(
            parse=lambda: get_port_cons.parse(
                args=[
                    self.test_scope,
                    self.test_portfolio,
                    self.test_date,
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_get_port_holdings(self):

        result = get_port_holdings.main(
            parse=lambda: get_port_holdings.parse(
                args=[
                    self.test_scope,
                    self.test_portfolio,
                    "--secrets-file",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_get_instr_id_types(self):

        result = get_instr_id_types.main(
            parse=lambda: get_instr_id_types.parse(
                args=["--secrets", f"{self.secrets_file}",]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_list_instruments(self):

        result = list_instruments.main(
            parse=lambda: list_instruments.parse(
                args=["--secrets", f"{self.secrets_file}",]
            ),
            display_df=self.display_df,
        )
        self.validate_results_df(result)

    def test_get_port_commands(self):

        result = get_port_commands.main(
            parse=lambda: get_port_commands.parse(
                args=[self.test_scope, "abc", "--secrets", f"{self.secrets_file}"]
            ),
            display_df=self.display_df,
        )
        self.validate_results_dict(result)

    def test_get_holdings_adj(self):

        result = get_holdings_adj.main(
            parse=lambda: get_holdings_adj.parse(
                args=[
                    self.test_scope,
                    self.test_portfolio,
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )

        self.validate_results_df(result)

    def test_get_transactions(self):

        result = get_transactions.main(
            parse=lambda: get_transactions.parse(
                args=[
                    self.test_scope,
                    self.test_portfolio,
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )

        self.validate_results_df(result)

    def test_search_instruments(self):

        result = search_instruments.main(
            parse=lambda: search_instruments.parse(
                args=[
                    "--properties",
                    "Instrument/default/Name=test",
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )

        self.validate_results_df(result)

    @unittest.skip("not implemented")
    def test_create_group_portfolios(self):

        data_file = (
            Path(__file__).parent.joinpath("data").joinpath("ExampleData.xlsx:Groups")
        )

        cgp.main(
            parse=lambda: cgp.parse(
                args=[
                    self.test_scope,
                    f"{data_file}",
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )

    def test_create_portfolio(self):

        portfolio = f"pf-{str(uuid.uuid4())}"

        up.main(
            parse=lambda: up.parse(
                args=[
                    self.test_scope,
                    portfolio,
                    "-c",
                    portfolio,
                    "GBP",
                    "2018-01-01",
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )

    @unittest.skip("not implemented")
    def test_upload_quotes(self):

        data_file = (
            Path(__file__).parent.joinpath("data").joinpath("ExampleData.xlsx:Quotes")
        )

        uq.main(
            parse=lambda: uq.parse(
                args=[
                    self.test_scope,
                    f"{data_file}",
                    "--secrets",
                    f"{self.secrets_file}",
                ]
            ),
            display_df=self.display_df,
        )


if __name__ == "__main__":
    unittest.main()
