import pandas as pd
import dateutil
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt import qry_instr_ids, lpt
from .either import Either
import re
import urllib.parse

rexp = re.compile(r".*page=([^=']{10,}).*")

TOOLNAME = "instr_list"
TOOLTIP = "List all instruments"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Query Instruments", ["filename", "limit"])
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    def list_instruments(ids):
        id_columns = {"identifiers.KEY:{}".format(v): v for v in ids["Id"].values}

        def fetch_page(page_token):
            return api.call.list_instruments(limit=3000, page=page_token)

        results = []

        def got_page(result):
            columns = ["lusid_instrument_id", "name"]
            columns.extend(sorted(id_columns.keys()))
            df = lpt.to_df(result, columns).dropna(axis=1, how="all")
            df.rename(columns=id_columns, inplace=True)

            results.append(df)

            links = [l for l in result.content.links if l.relation == "NextPage"]

            if len(links) > 0:
                match = rexp.match(links[0].href)
                if match:
                    print("{} {}".format(len(results), match.group(1)))
                    return urllib.parse.unquote(match.group(1))
            return None

        page = Either(None)
        while True:
            page = fetch_page(page.right).bind(got_page)
            if page.is_left():
                return page
            if page.right == None:
                break

        return lpt.trim_df(
            pd.concat(results, ignore_index=True, sort=False), args.limit, sort="name"
        )

    return qry_instr_ids.process_args(api, args).bind(list_instruments)


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    return lpt.standard_flow(parse, lse.connect, process_args, display_df)
