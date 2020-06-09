import os

import pandas as pd

from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.either import Either

mapping_prefixes = {"Figi": "FIGI", "ClientInternal": "INT", "QuotePermId": "QPI"}

mapping_table = {}


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Map Instruments", ["filename"])
        .add("--folder", help="include all 'txn' files in the folder")
        .add("input", nargs="*", metavar="file", help="file(s) containing instruments")
        .add(
            "--column",
            metavar="input-column",
            default="instrument_uid",
            help="column name for instrument column",
        )
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    if args.folder:
        args.input.extend(
            [
                os.path.join(args.folder, f)
                for f in os.listdir(args.folder)
                if "-txn-" in f and f.endswith(".csv")
            ]
        )

    df = (
        pd.concat(
            [lpt.read_csv(f)[[args.column]].drop_duplicates() for f in args.input],
            ignore_index=True,
            sort=True,
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    df.columns = ["FROM"]
    df["TO"] = df["FROM"]

    return map_instruments(api, df, "TO")


def main():
    lpt.standard_flow(parse, lse.connect, process_args)


def map_instruments(api, df, column):
    WORKING = "__:::__"  # temporary column name

    # Apply any known mappings to avoid unecessary i/o

    if len(mapping_table) > 0:
        srs = df[column].map(mapping_table)
        srs = srs[srs.notnull()]
        if len(srs) > 0:
            df.loc[srs.index, column] = srs

    # updates the mappings table
    def update_mappings(src, prefix):
        mapping_table.update(
            {prefix + k: v.lusid_instrument_id for k, v in src.items()}
        )

    def batch_query(instr_type, prefix, outstanding):
        if len(outstanding) > 0:
            batch = outstanding[:500]  # records to process now
            remainder = outstanding[500:]  # remaining records

            # called if the get_instruments() succeeds
            def get_success(result):
                get_found = result.content.values
                get_failed = result.content.failed

                # Update successfully found items
                update_mappings(get_found, prefix)

                if len(get_failed) > 0:
                    if instr_type == "ClientInternal":
                        # For un-mapped internal codes, we will try to add (upsert)

                        # called if the upsert_instruments() succeeds
                        def add_success(result):
                            add_worked = result.content.values
                            add_failed = result.content.failed

                            if len(add_failed) > 0:
                                return Either.Left("Failed to add internal instruments")

                            # Update successfully added items
                            update_mappings(add_worked, prefix)

                            # Kick off the next batch
                            return batch_query(instr_type, prefix, remainder)

                        # Create the upsert request from the failed items
                        request = {
                            k: api.models.InstrumentDefinition(
                                name=v.id, identifiers={"ClientInternal": v.id}
                            )
                            for k, v in get_failed.items()
                        }

                        return api.call.upsert_instruments(request).bind(add_success)
                    else:
                        # Instruments are not mapped. Nothing we cando.
                        return Either.Left(
                            "Failed to locate instruments of type {}".format(instr_type)
                        )
                else:
                    # No failures, kick off the next batch
                    return batch_query(instr_type, prefix, remainder)

            return api.call.get_instruments(
                instr_type, list(batch[WORKING].values)
            ).bind(get_success)
        else:
            # No records remaining. Return the now-enriched dataframe
            return Either.Right(df)

    def map_type(key, instr_type):
        prefix = key + ":"
        subset = df[df[column].str.startswith(prefix)]

        # See if there are any entries of this type
        if len(subset) > 0:
            width = len(prefix)
            uniques = subset[[column]].drop_duplicates(column)
            uniques[WORKING] = uniques[column].str.slice(width)

            def map_success(v):
                df.loc[subset.index, column] = subset[column].map(mapping_table)
                return Either.Right(df)

            return batch_query(instr_type, prefix, uniques).bind(map_success)
        else:
            # Nothing to be done, pass the full result back
            return Either.Right(df)

    return (
        map_type("FIGI", "Figi")
        .bind(lambda r: map_type("INT", "ClientInternal"))
        .bind(lambda r: map_type("QPI", "QuotePermId"))
    )


def include_mappings(path):
    if path:
        df = lpt.read_csv(path)

        mapping_table.update(df.set_index("FROM")["TO"].to_dict())
