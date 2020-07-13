import re
import pandas as pd
from collections import defaultdict
from functools import reduce
from .record import Rec
from .either import Either
from . import back_compat

type_re = re.compile(r"(.*)\((.*),(.*)\)")


# Convert an iterable dataset to a DataFrame
def to_df(data, columns):
    if isinstance(data, Rec):
        data = data.content

    # Create record by accessing each column
    def to_record(obj):
        return {col: access(obj, col) for col in columns}

    def property_value(p):
        return p.metric_value.value if p.label_value is None else p.label_value

    # traverse dot notation to flatten sub-objects
    def access(obj, col):
        if col.startswith("P:"):
            try:
                props = getattr(obj, "properties")
                if type(props) == list:
                    return property_value(
                        [p.value for p in props if p.key == col[2:]][0]
                    )
                return property_value(props[col[2:]].value)
            except:
                return None

        if col.startswith("SHK:"):
            try:
                props = getattr(obj, "sub_holding_keys")
                return property_value(props[col[4:]].value)
            except:
                return None

        for fld in col.split("."):
            if fld.startswith("KEY:"):
                obj = obj.get(fld[4:]) if obj else None
            else:
                obj = getattr(obj, fld) if obj else None
        return obj

    # Try standard representations
    try:
        iterator = iter(data)
    except:
        iterator = iter(data.values)

    records = [to_record(o) for o in iterator]

    if len(records) == 0:
        return pd.DataFrame({col: [] for col in columns})[columns]

    return pd.DataFrame.from_records(records)[columns]


# Utilities to convert YYYY-MM-DD strings to and from UTC dates.
def to_date(date, **kwargs):
    return pd.to_datetime(date, utc=True, **kwargs) if date is not None else None


def from_date(date):
    return date.strftime("%Y-%m-%d") if date else None


def add_days(date, days):
    return to_date(date) + pd.Timedelta("{} days".format(days))


# Display a dataframe with no cropping
def display_df(df, decimals=2):
    fmt = "{:,." + str(decimals) + "f}"
    pd.options.display.float_format = fmt.format
    pd.set_option("max_colwidth", None)

    try:
        if len(df) == 1 and len(df.columns) > 5:
            df = df.T
        with pd.option_context("display.width", None, "display.max_rows", 1000):
            print(df.fillna(""))
    except:
        print(df)


# Create API objects from a dataframe
def from_df(
    df, record_type, complex_types, related=None, columns=None, date_fields=None
):
    simple_columns = []
    complex_columns = defaultdict(list)
    properties = []

    date_fields = set(
        (date_fields or [])
        + [k for (k, v) in record_type.openapi_types.items() if v == "datetime"]
        + [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    )

    if len(date_fields) > 0:
        df = df.copy()
        for col in date_fields:
            df[col] = pd.to_datetime(df[col], utc=True).map(
                lambda x: None if pd.isna(x) else x
            )

    for col in columns or df.columns.values:
        if col.startswith("P:"):
            properties.append(col[2:])
        elif "." in col:
            (k, v) = col.split(".")
            complex_columns[k].append(v)
        else:
            simple_columns.append(col)

    def build_complex_type(row, col, fields):
        d = {f: row["{}.{}".format(col, f)] for f in fields}
        col_type = complex_types[record_type.openapi_types[col]]
        return col_type(**d)

    def build_properties(row):
        ptype_tpl = type_re.findall(record_type.openapi_types["properties"])[0]

        ptype = complex_types[ptype_tpl[2].strip()]

        def prop_builder(property_key, value):
            if isinstance(value, str):
                return ptype(
                    key=property_key,
                    value=complex_types["PropertyValue"](label_value=value),
                )
            elif pd.isna(value) == False:
                return ptype(
                    key=property_key,
                    value=complex_types["PropertyValue"](
                        metric_value=complex_types["MetricValue"](value)
                    ),
                )
            else:
                return None

        props = [
            (col, prop_builder(col, row["P:{}".format(col)])) for col in properties
        ]
        d = dict([p for p in props if p[1] != None])
        return d

    def to_type(i, row):
        fields = {col: row[col] for col in simple_columns}
        fields.update(
            {
                col: build_complex_type(row, col, fields)
                for col, fields in complex_columns.items()
            }
        )

        if "properties" in record_type.openapi_types and len(properties) > 0:
            fields["properties"] = build_properties(row)

        if related != None:
            if callable(related):
                fields = related(i, row, fields)
            else:
                # Dict type
                fields.update(related.get(i, {}))

        # Quick and dirty instrument_uid handling
        # This should change to allow the full
        # instrument resolution logic to apply
        if "instrument_uid" in fields.keys():
            fields["instrument_identifiers"] = to_instrument_identifiers(
                fields["instrument_uid"]
            )
            del fields["instrument_uid"]

        # Remove any 'noise' from the dataframe
        allowed = set(record_type.openapi_types.keys())
        trimmed = dict([tpl for tpl in fields.items() if tpl[0] in allowed])

        return record_type(**trimmed)

    return [to_type(i, row) for (i, row) in df.iterrows()]


def to_instrument_identifiers(uid):
    if uid.startswith("CCY_"):
        return {"Instrument/default/Currency": uid[4:]}
    if uid.startswith("Ccy:"):
        return {"Instrument/default/Currency": uid[4:]}
    if uid.startswith("Currency:"):
        return {"Instrument/default/Currency": uid[9:]}
    elif uid.startswith("ClientInternal:"):
        return {"Instrument/default/ClientInternal": uid[15:]}
    elif uid.startswith("Figi:"):
        return {"Instrument/default/Figi": uid[5:]}
    elif uid.startswith("RIC:"):
        return {"Instrument/default/RIC": uid[4:]}
    else:
        parts = uid.split(":")
        if len(parts) == 2 and parts[0].startswith("Instrument"):
            return {parts[0]: parts[1]}
        return {"Instrument/default/LusidInstrumentId": uid}


# Convert iterable of Record to DataFrame
def records_to_df(records):
    return pd.DataFrame([r.to_dict() for r in records])


# Break a dataframe down into batches
def chunk(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


# Write statistics out
def dump_stats(filename, stats, columns):
    if len(stats) > 0:
        df = records_to_df(stats)[columns]
        if filename == "-":
            display_df(df.drop(["startTime", "endTime"], axis=1))
        else:
            df.to_csv(filename, index=False)


# Limit the length of DataFrame
def trim_df(df, limit, **kwargs):
    if kwargs.get("sort", None) != None:
        df = df.sort_values(kwargs["sort"]).reset_index(drop=True)
    return df[:limit] if 0 < limit < len(df) else df


# Template 'program'
def standard_flow(parser, connector, executor, display_df=display_df):
    args = parser()
    api = connector(args)

    either = Either(executor(api, args))

    # called if the executor returns a success
    def success(df):
        # Query type programs will return a dataframe
        if df is not None:
            fn = args.__dict__.get("filename", None)
            if fn is not None:
                if ".xls" in fn.lower():
                    df.to_excel(fn, index=False)
                elif fn.endswith(".pk"):
                    df.to_pickle(fn)
                else:
                    df.to_csv(fn, index=False)
            else:
                if "dfq" in args and args.dfq:
                    from . import dfq

                    dfq.dfq(dfq.parse(False, args.dfq), df)
                else:
                    return display_df(df)

    return either.match(left=display_error, right=success)


# Nicely display an error from LUSID
def display_error(error, response=False):
    try:
        print(
            "ERROR: {} Reason:{}, Code:{}\n".format(
                error.status, error.reason, error.code
            )
        )
        print("MESSAGE: {}\n".format(error.message))
        print("DETAILS: {}\n".format(error.detailed_message))

        if len(error.items) > 0:
            df = records_to_df(
                [Rec(Id=key, Detail=item) for key, item in error.items.items()]
            )
            print("ITEMS (max 50)")
            display_df(df[:50])
    except:
        print(str(error))
    return response


# backwards compatibility
def read_csv(path, frame_type=None, **kwargs):
    return read_input(path, frame_type, **kwargs)


# Read in a data-file and apply any backwards compatibility settings
def read_input(path, frame_type=None, mappings=None, **kwargs):
    sheet = kwargs.get("sheet_name", 0)
    if is_path_supported_excel_with_sheet(path):
        path, sheet = path.rsplit(":", 1)

    if ".xls" in path.lower():
        df = pd.read_excel(path, sheet_name=sheet, **kwargs)
    else:
        df = pd.read_csv(path, **kwargs)

    if mappings is not None:
        df = df.rename(columns=mappings)
        df = df[list(set(mappings.values()) & set(df.columns))]

    return back_compat.convert(frame_type, df)


# Check if a path is a supported excel file with a suffixed sheet
def is_path_supported_excel_with_sheet(path):
    return re.match(".*\.(xls|xlsx|xlsm|xlsb):", path)


# Create PerpetualProperties request from prefixed columns in a dataframe
def perpetual_upsert(models, df, prefix="P:"):
    offset = len(prefix)

    def make_property(properties, key):
        return properties + [
            (
                key[offset:],
                models.PerpetualProperty(
                    key[offset:], models.PropertyValue(label_value=str(value))
                ),
            )
            for value in df[key].dropna().head(1).tolist()
        ]

    return dict(
        reduce(
            make_property, [c for c in df.columns.values if c.startswith(prefix)], []
        )
    )


# Return the serialised representation of the object
def Serialise(api, body, bodyType):
    return api.api._serialize.body(body, bodyType)


def process_input(aliases, api, args, fn):
    df = pd.concat(
        [read_input(input_file, dtype=str) for input_file in args.input],
        ignore_index=True,
        sort=False,
    )
    if args.mappings:
        df.rename(
            columns=dict(
                [
                    (s[1], aliases.get(s[0], s[0]))
                    for s in [m.split("=") for m in args.mappings]
                ]
            ),
            inplace=True,
        )
    prop_keys = [col for col in df.columns.values if col.startswith("P:")]
    identifiers = [col for col in df.columns.values if col in args.identifiers]
    # Identifiers have to be unique
    df = df.drop_duplicates(identifiers)

    return fn(api, args, df, identifiers, prop_keys)
