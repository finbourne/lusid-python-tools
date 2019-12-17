# Lusidtools Documentation

Lusidtools is an opensource project from Finbourne technology that handles data uploads to LUSID.

[TOC]

Our product, LUSID, is changing the way the world manages investment data. It is an open, cloud-native advanced investment management platform that delivers superior technology, with more choice, at a far lower cost. Our central ambition is to liberate you and the rest of the financial community from the limitations of your systems, so you can:

- Focus on investment not technology

- Reduce the cost of change

- Grow and evolve faster

- Control your data

- Win more clients and keep them

If you haven’t already done so, you can learn more about LUSID and its benefits by visiting [www.finbourne.com/lusid](www.finbourne.com/lusid). 

Go to the [Sign-up page](https://demosetup.lusid.com/app/resources/tutorials/getting-started/create-account/create-domain) to create your own LUSID domain today. 

The [API Documentation]() and [Specification in swagger]() are two useful resources that provide in-depth documentation and allow authorised clients to query  and update their data directly. 

SDK's to interact with the LUSID API on GitHub are available in the following languages:

- C#
- Java
- JavaScript
- Python

The Lusidtools package was built as a quick reference implementation of daily operations using python3.

All calls to LUSID are made in a single line of code using the `load_from_data_frame()` function call. The mapping of source data columns to LUSID properties can be configured as a single structure for a given file, and even supports multiple operations from a single file such as Loading instruments, transactions and generating holdings from a single transactions file. 


The package also contains a number of utility functions that can be used to process data if required. 



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



# Instrument properties

Additional instrument properties can be applied to the instruments. An example of this is **strategy tagging**.

| inst_name      | FIGI         | Ticker  | Market Sector | Currency | Security type | Strategy |
| -------------- | ------------ | ------- | ------------- | -------- | ------------- | -------- |
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  | Evil     |
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  | Good     |
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  | Evil     |

​	The required mapping for upserting instrument properties can be found in the online API documentation [Upsert Instrument Properties](https://www.lusid.com/docs/api/operation/UpsertInstruments#operation/UpsertInstrumentsProperties) 

```python
mapping["strat_mapping"] = {
    "required": {
        "identifier": "FIGI",
	    "identifier_type": "$Figi"
    },
    "properties": ["Strategy"]
}
```

```python
# load strategy tags
strat_properties = pd.read_csv("path/to/strat/tags.csv")

result = lpt.cocoon.load_from_data_frame(
    api_factory=api_factory,
    scope=scope,
    data_frame=strat_properties,
    mapping_required=mapping["strat_mapping"]["required"],
    mapping_optional={},
    file_type="instrument_property",
    property_columns=mapping["strat_mapping"]["properties"],
    properties_scope=scope
)
```



# Transaction Portfolios

portfolios can be created from a transactions file using the portfolio codes and descriptions. The required fields can be found in the documentation for [Transaction portfolios](https://www.lusid.com/docs/api/operation/UpsertInstruments#operation/CreatePortfolio). 

| inst_name      | portfolio | description | FIGI         | Ticker  | Market Sector | Currency | Security type | Strategy |
| -------------- | --------- | ----------- | ------------ | ------- | ------------- | -------- | ------------- | -------- |
| Apple          | tech_001  | Big Corps   | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  | Evil     |
| MICROSOFT CORP | tech_001  | Big Corps   | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  | Good     |
| UBER Tech      | tech_002  | Disruptors  | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  | Evil     |

```python
mapping["portfolio_mapping"] = {
  "required": {
      "code": "portfolio",
      "display_name": "portfolio name",
      "base_currency": {
          "default": "USD",
          "column": "Account Base Currency"
      },
      "created": "$2018-01-01T00:00:00+00:00"
  },
```

```python
# Load data
portfolios = pd.read_csv("path/to/data/with/portfoliocodes.csv")

result = lpt.cocoon.load_from_data_frame(
    api_factory=api_factory,
    scope=scope,
    data_frame=portfolios,
    mapping_required=mapping["portfolios"]["required"],
    mapping_optional={},
    file_type="portfolios"
)
```



# Transactions

The next stage is to populate the portfolio by including all transactions from inception to date. 

TODO: transaction type configuration

| inst_name      | portfolio | FIGI         | Tx_id   | Price | Currency | amount | units | effective_date | settle_date | Broker Name |
| -------------- | --------- | ------------ | ------- | ----- | -------- | ------ | ----- | -------------- | ----------- | ----------- |
| Apple          | tech_001  | BBG0013T5HY0 | f2g1f45 | 51    | USD      | 255    | 500   | 18/11/2019     | 20/11/2019  | B1          |
| MICROSOFT CORP | tech_001  | BBG007F5XJZ0 | d55sdf5 | 65    | USD      | 245    | 250   | 18/11/2019     | 18/11/2019  | B2          |
| UBER Tech      | tech_002  | BBG00NW4HSM1 | o5g18fw | 49    | USD      | 448    | 700   | 18/11/2019     | 24/11/2019  | B1          |

```python
mapping["transactions"] = {
        "identifier_mapping": {
            "Figi": "FIGI",
        },
        "required": {
            "code": "portfolio",
            "transaction_id": "Tx_id",
            "type": "Transaction Code",
            "transaction_price.price": "Price",
            "transaction_price.type": "$Price",
            "total_consideration.amount": "amount",
            "units": "units",
            "transaction_date": "effective_Date",
            "transaction_currency": "Currency",
            "total_consideration.currency": "Currency",
            "settlement_date": "settle_date"
        },
        "optional": {
            "source": "$DataProvider"
        },
        "properties": [
            "Broker Name"
        ]
    }
```

TODO: comment on cash items on transactions. Why do we need to Identify cash items

```python
# Load data
txn_df = pd.read_csv("path/to/data/with/transactions.csv")

# Identify cash items and label as currency
txn_df, mapping = lpt.identify_cash_items(dataframe=txn_df.copy(), file_type="transactions", mappings=mapping)

# 
result = lpt.cocoon.load_from_data_frame(
    api_factory=api_factory,
    scope=scope,
    data_frame=txn_df,
    mapping_required=mapping["transactions"]["required"],
    mapping_optional=mapping["transactions"]["optional"],
    file_type="transactions",
    identifier_mapping=mapping["transactions"]["identifier_mapping"],
    property_columns=mapping["transactions"]["properties"],
    properties_scope=scope
)
```



# Holdings

Holdings can be set and adjusted directly from a positions file. 

See the documentation for [Set Holdings/Adjust Holdings](https://www.lusid.com/docs/api/operation/UpsertInstruments#operation/SetHoldings) for more information of required and optional fields. 

| inst_name      | portfolio | FIGI         | Price | Currency | amount | units | effective_date | settle_date | Broker Name |
| -------------- | --------- | ------------ | ----- | -------- | ------ | ----- | -------------- | ----------- | ----------- |
| Apple          | tech_001  | BBG0013T5HY0 | 51    | USD      | 255    | 500   | 18/11/2019     | 20/11/2019  | B1          |
| MICROSOFT CORP | tech_001  | BBG007F5XJZ0 | 65    | USD      | 245    | 250   | 18/11/2019     | 18/11/2019  | B2          |
| UBER Tech      | tech_002  | BBG00NW4HSM1 | 49    | USD      | 448    | 700   | 18/11/2019     | 24/11/2019  | B1          |

```python
mapping["holdings"]:{
    "required":{
        "code": "Portfolio",
        "effective_at": "effective_date",
        "tax_lots.units": "units"
    },
    "identifier_mapping": {
        "identifier_mapping": {
            "Figi": "FIGI"
        }
    },
    "optional": {
        "tax_lots.cost.amount": "amount",
        "tax_lot.cost.currency": "Currency",
        "tax_lots.price": "Price"
    }
}
```

```python
seg_df, holdings_mapping = lpt.identify_cash_items(
    dataframe=seg_fund.copy(), 
    mappings=holdings_mappings, 
    file_type="holdings", 
    remove_cash_items=True
)

result = lpt.load_from_data_frame(
    api_factory=api_factory,
    scope=holdings_scope,
    data_frame=seg_df,
    mapping_required=mapping["holdings"]["required"],
    mapping_optional=mapping["holdings"]["optional"],
    identifier_mapping=holdings_mapping["holdings"]["identifier_mapping"],
    file_type="holdings"
)
```

# Quotes

In order to perform a valuation, LUSID must have a set of quotes available in the scope. If current Quotes have not been provided then LUSID chooses the most recently added quotes. The documentation for Upserting Quotes can be found [here](https://www.lusid.com/docs/api/operation/UpsertInstruments#operation/UpsertQuotes).

| name           | FIGI         | Price | Price Code |      |
| -------------- | ------------ | ----- | ---------- | ---- |
| Apple          | BBG0013T5HY0 | 50    | Stock      |      |
| MICROSOFT CORP | BBG007F5XJZ0 | 62    | Stock      |      |
| UBER Tech      | BBG00NW4HSM1 | 24    | Stock      |      |
| Gvmnt 2020     | BBG00UF9KSP9 | 99.73 | Bond       |      |

```python
mapping["quotes"] = {
        "required": {
            "quote_id.quote_series_id.instrument_id_type": "$Figi",
            "quote_id.effective_at": "$2019-11-20T00:00:00Z",
            "quote_id.quote_series_id.provider": "$DataProvider",
            "quote_id.quote_series_id.field": "$mid",
            "quote_id.quote_series_id.quote_type": "$Price",
            "quote_id.quote_series_id.instrument_id": "FIGI",
            "metric_value.unit": "Currency",
            "metric_value.value": "__adjusted_quote"
        },
        "quote_scalar":{
            "price": "Price",
            "type": "Price Code",
            "type_code": "Bond",
            "scale_factor": 0.01
        }
    }
```



This set of quotes includes bonds, who's prices are quoted as value of a bond, instead of share/par. The utility function `scale_quote_of_type()` takes the quote_scalar mapping configuration and uses it to adjust the prices for bonds by a provided scale factor. 

```python
# Load data
df_quotes = pd.read_excel("data/initial-data-load/Fund Accounting Detailed Positions Report with SEG1.xlsx")

# Scale prices for Bonds using lusittools utility function
df_adjusted_quotes = lpt.cocoon.cocoon.scale_quote_of_type(copy.deepcopy(df_quotes), mapping)

result = lpt.load_from_data_frame(
    api_factory=api_factory,
    scope=scope,
    data_frame=df_adjusted_quotes,
    mapping_required=mapping["quotes"]["required"],
    mapping_optional={},
    file_type="quotes"
)
```

# Example mapping structure

Below is an example of what a fully populates mapping structure could look like: 

```json
mapping = {
    "instruments":{
        "identifier_mapping": {
            "ClientInternal": "Security Number",
            "Ticker": "Ticker Symbol",
            "Cusip": "CUSIP",
            "Sedol": "SEDOL",
            "Isin": "ISIN",
            "RIC": "Reuters RIC"
        },
        "required": {
            "name": "Security Description"
        },
        "properties":[
            "Security Category",
            "Security Category Sector",
            "Trading Currency",
            "Issue Currency",
            "Country of Taxation",
            "Country of Risk",
        ]
    },
    "transactions": {
        "identifier_mapping": {
            "ClientInternal": "Security Number",
            "Figi": "FIGI"
        },
        "required": {
            "code": "Account ID code",
            "transaction_id": "trans_id",
            "type": "Transaction Type",
            "transaction_price.price": "Price",
            "transaction_price.type": "$Price",
            "total_consideration.amount": "Amount",
            "units": "Shares/Par",
            "transaction_date": "Effective Date",
            "transaction_currency": "Trading Currency",
            "total_consideration.currency": "Settlement Currency",
            "settlement_date": "Settlement Date"
        },
        "optional": {
            "source": "$NT-InvestOne"
        },
        "properties": [
            "Transaction Description",
            "Broker Name ",
            "Master ID"
        ]
    },
    "quotes": {
        "required": {
            "quote_id.quote_series_id.instrument_id_type": "$Figi",
            "quote_id.effective_at": "$2019-11-13T00:00:00Z",
           "quote_id.quote_series_id.provider": "$DataScope",
           "quote_id.quote_series_id.field": "$mid",
           "quote_id.quote_series_id.quote_type": "$Price",
           "quote_id.quote_series_id.instrument_id": "FIGI",
           "metric_value.unit": "Currency",
           "metric_value.value": "__adjusted_quote"
        },
        "quote_scalar":{
            "price": "Price",
            "type": "Price Code",
            "type_code": "Bond",
            "scale_factor": 0.01
        }
    },
    "portfolios": {
        "required": {
            "code": "Portfolio",
            "display_name":  "Account Name",
            "base_currency": {
                    "default": "GBP",
                    "column": "Account Base Currency"
            },
            "created": "$2018-01-01T00:00:00+00:00"
        }
    },
        "holdings": {
        "required": {
            "code": "Portfolio",
            "effective_at": "$2018-11-20T00:00:00+00:00",
            "tax_lots.units": "units"
        },
        "optional": {
            "tax_lots.cost.amount": "Amortized Cost (Local)",
            "tax_lots.cost.currency": "Trading Currency",
            "tax_lots.portfolio_cost": "Amortized Cost (Base)",
            "tax_lots.price": "Price (Local)"        
        },
        "identifier_mapping": {
            "ClientInternal": "Security Number"
        }
    },
    "portfolios": {
        "required": {
            "code": "Portfolio",
            "display_name":  "Account Name (Short)",
            "base_currency": {
                    "default": "GBP",
                    "column": "Account Base Currency"
            },
            "created": "$2018-01-01T00:00:00+00:00"
        }
    },
    "cash_flag": {
        "cash_identifiers": {
            "FA Asset Group" : ["CC", "CU", "CA"]
        },
        "implicit": "Trading Currency"
    }
}
```



# lusidtools.cocoon.load_from_data_frame()

```python
lusidtools.load_from_data_frame(
    api_factory: lusid.utilities.ApiClientFactory, 
    scope: str, 
    data_frame: pd.DataFrame, 
    mapping_required: dict, 
    mapping_optional: dict, 
    file_type: str, 
    identifier_mapping: dict = None, 
    property_columns: list = None, 
    properties_scope: str = None, 
    batch_size: int = None,
    remove_white_space: bool = True, 
    instrument_name_enrichment: bool = False
):
```



`Load_from_data_frame()` handles uploading data from source files into LUSID.

## Parameters:

### API Factory

The api factory takes the authenticated credentials of the client. This can be constructed using 3 methods:

- secrets.json file containing credentials
- Environment Variables
- bearer token used to initialise API

See LUSID's support article on [Getting started with the LUSID API and SDKs](https://support.lusid.com/getting-started-with-apis-sdks) to create a secrets JSON file or set your machine's environment variables. 

#### Using local secrets JSON (Recommended for running lusidtools locally)

```python
Api_factory =  ApiClientFactory(api_secrets_filename="c:full/path/to/secrets.json")
```

#### Using credentials set as Environment Variables 

```python
Api_factory =  ApiClientFactory()
```

#### Using Bearer Token (recommended for running lusidtools from an authenticated browser eg.hoasted jupyterhub instance)

Using web token from jupyter hub:

```python
ApiClientFactory(token=lusid_sample_data.RefreshingToken(), api_url=os.getenv("FBN_LUSID_API_URL", None), app_name="Jupyter")
```

### scope

The scope of the resource to load the data into. See [this support article](https://support.lusid.com/what-is-a-scope-in-lusid-and-how-is-it-used) to understand more about how scopes are used in LUSID.

### Dataframe 

The DataFrame containing data. This could be instrument

### Identifier mapping 

The dictionary mapping of LUSID instrument identifier types to the column headers of identifiers in the DataFrame.

Extract from Dataframe:

| name (long)  | internal ID | id_isin      | ticker |
| ------------ | ----------- | ------------ | ------ |
| APPLE (AAPL) | SS0000001   | US0378331005 | AAPL   |

Corresponding mapping:


```json
{
    "ClientInternal": "internal ID",
    "Isin": "id_isin",
    "ticker": "ticker"
}
```

### Required mapping

the dictionary mapping the DataFrame columns to LUSID's required attributes

The required mapping provides the mapping for the parameters marked as **required** for the LUSID API endpoints. The required fields for each API call can be found in the LUSID [documentation](https://www.lusid.com/docs/api/). 

### Optional mapping

The dictionary mapping the DataFrame columns to LUSID's optional attributes


### File Type

The type of file e.g. transactions, instruments, holdings, quotes, portfolios


### Property Columns

A list of property column names from the dataframe to be uploaded as properties. See this article on [properties in LUSID](https://support.lusid.com/what-is-a-property) for more information on properties.

Extract from Dataframe:

| Country of Taxation | Country of Risk | Security Category | Security Category Industry |
| ------------------- | --------------- | ----------------- | -------------------------- |
| United Kingdom      | United Kingdom  | A                 | Energy                     |

Corresponding mapping:

```json
[
    Country of Taxation,
    Country of Risk,
    Security Category, 
    Security Category Industry
]
```

### Properties Scope
The name of the scope in which to upsert properties to.

### Batch size

Batch size explicitly states the size of the batch to use when using upserting data e.g. upsert instruments, upsert quotes etc.

### Remove white space

When set to True, this will remove unintended whitespace in the dataframe.

### Instrument name enrichment

When set to True, this will request additional information from open-figi to be added when upserting instruments.

# Utility Functions

## identify_cash_items()

The cash_flag mapping field is used by `identify_cash_items()` to identify cash items and either remove them, or mark them as currencies.

The example below shows how `identify_cash_items()` uses the cash_flag mapping to perform its two functions:

- removing cash from a dataframe
- Labelling items in a dataframe as Currencies 

| inst_name      | FIGI_id      | Ticker  | Market Sector | Currency | Security type |
| -------------- | ------------ | ------- | ------------- | -------- | ------------- |
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  |
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  |
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  |
| USD            |              | USD     |               | USD      | Cash          |
| JPY            |              | JPY     |               | JPY      | Currency      |

```python#
mapping = {
    "instruments": {
        "required": {"name": "inst_name"},
        "identifier_mapping": {"Figi": "FIGI"},
    },
    "cash_flag": {
        "cash_identifiers": {
            "Security type" : ["Cash", "Currency"]
        },
        "implicit": "Currency"
    }
}
```

#### dataframe_cash_removed

Setting `remove_cash_items` to True removes any cash from the dataframe. The mapping returned remains unchanged.

```python
dataframe_cash_removed, mapping = identify_cash_items(
    dataframe = dataframe.copy(), 
    mappings = mapping, 
    file_type = "instruments", 
    remove_cash_items = True
)
```

| inst_name      | FIGI_id      | Ticker  | Market Sector | Currency | Security type |
| -------------- | ------------ | ------- | ------------- | -------- | ------------- |
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  |
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  |
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  |

#### dataframe_cash_labled_as_currency

Setting `remove_cash_items` to False labels creates a new column `__currency_identifier_for_LUSID` which is assigned a Currency identifier for any cash items. The mapping for this new field is then appended to the identifier mapping. 

```python
dataframe_cash_labled_as_currency, mapping = identify_cash_items(
    dataframe = dataframe.copy(), 
    mappings = mapping, 
    file_type = "instruments", 
    remove_cash_items = False
)
```

| inst_name      | FIGI_id      | Ticker  | Market Sector | Currency | Security type |__currency_identifier_for_LUSID|
| -------------- | ------------ | ------- | ------------- | -------- | ------------- |--|
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  |None|
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  |None|
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  |None|
| USD            |              | USD     |               | USD      | Cash          |USD|
| JPY            |              | JPY     |               | JPY      | Currency      |JPY|



## scale_quote_of_type()



# Mapping config

The highest level of the mapping structure can contain any of the supported file types that don't have contradicting column names. For example, if the instruments master, transactions and quotes files all contain the same format , their mappings can be represented in a single mapping dictionary. If however the files contain contradicting column names (e.g. Instrument figi column is titled "figi", but in the transaction file it it ladled "Instrument_id_FIGI") then different mapping dictionaries are required.

**cash_flag** contains information that can be used to identify rows containing cash. This will become significant when upserting instruments and transactions.



```python
mapping = {
    "instruments": {},
    "instrument_properties": {}
    "transactions": {},
    "holdings": {},
    "quotes":{},
    "cash_flag": {}
}
```
