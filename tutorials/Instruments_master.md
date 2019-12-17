# Instruments master

This section demonstrates how instruments can be loaded into LUSID from a source file. This file could be a securities master, or any other file containing instrument data such as a transactions file. 

The table below shows an extract from a typical instruments file.

| inst_name      | FIGI_id      | Ticker  | Market Sector | Currency | Security type |
| -------------- | ------------ | ------- | ------------- | -------- | ------------- |
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  |
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  |
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  |
| USD            |              | USD     |               | USD      | Cash          |
| JPY            |              | JPY     |               | JPY      | Currency      |

In order to load the instrument data, each field on the request must be mapped to its appropriate column in the source data. For example, the Figi identifier in this file is labeled as "FIGI_id" and must be mapped to the "Figi" parameter in our request. The required fields can be found in the API documentation for  [Upsert Instruments](https://www.lusid.com/docs/api/operation/UpsertInstruments#operation/UpsertInstruments).

In this example the source file also contains some cash items which have a security type of "Cash" or "Currency".  The utility function `identify_cash_items()` is used here to identify cash by searching for specific values in a field, then assigning them a *__currency_identifier_for_LUSID* inherited from the source currency column. The configuration for this is provided in the *cash_flag* mapping, for more information on the behaviour, see documentation for `identify_cash_items()`.

```python 
mapping ={
    "instrument": {
        "identifier_mapping": {
            "Figi": "FIGI_id",
            "Ticker": "Ticker"
        }
        "required": {
            "name": {
                "column": "inst_name",
                "default": "Name Not Found"
            }
        },
        "properties": ["market Sector"]
    }
    "cash_flag": {
        "cash_identifiers": {
            "Security type": ["Cash", "Currency"]
        }
        "implicit": "currency"
    }
}
```

```python
from lusid.utilities import ApiClientFactory
import lusid_sample_data as sd
from lusidtools.cocoon import cocoon_printer as cp
import pandas as pd
import os
# get client
Api_factory =  ApiClientFactory(api_secrets_filename="c:full/path/to/secrets.json")

# load data
df = pd.load_excel("/path/to/data.XLSX")

# Specify scope
scope = "test_scope_1"

# Identify cash items 
instruments, mapping = identify_cash_items(df.copy(),
                                           mappings,
                                           "instruments")

result = lusidtools.cocoon.load_from_data_frame(
    api_factory=api_factory,
    scope=scope,
    data_frame=instruments,
    mapping_required=mapping["instruments"]["required"],
    mapping_optional={},
    file_type="instruments",
    identifier_mapping=mapping["instruments"]["identifier_mapping"],
    property_columns=mapping["instruments"]["properties"],
    properties_scope=scope
)
```

