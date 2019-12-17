# Lusidtools Documentation

Lusidtools is an opensource project from Finbourne technology that handles data uploads to LUSID.

If you haven’t already done so, you can learn more about LUSID and its benefits by visiting [www.finbourne.com/lusid](www.finbourne.com/lusid). 

Go to the [Sign-up page](https://demosetup.lusid.com/app/resources/tutorials/getting-started/create-account/create-domain) to create your own free trial LUSID domain in order to interact with LUSID. 

The [API Documentation]() and [Specification in swagger]() are two useful resources that provide in-depth documentation and allow authorised clients to query  and update their data directly. 

SDK's to interact with the LUSID API on GitHub are available in the following languages:

- C#
- Java
- JavaScript
- Python

The Lusidtools package was built as a quick reference implementation of daily operations using python3.

All calls to LUSID are made in a single line of code using the `load_from_data_frame()` function call. The mapping of source data columns to LUSID properties can be configured as a single structure for a given file, and even supports multiple operations from a single file such as Loading instruments, transactions and generating holdings from a single transactions file. 


The package also contains a number of utility functions that can be used to process data if required. 

# Tutorials

Example implementations can be found in `lusid-python-tools/tutorials/` for Loading the following file types into LUSID:

- Instrument Master
- Instrument Properties
- Transaction Portfolios
- Transactions
- Holdings
- Quotes

# lusidtools.cocoon.load_from_data_frame()

All data uploads are handled by this function call which is configured using a mapping structure. An example of this mapping structure can be found in the tutorials/ folder. 

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

###### Using local secrets JSON (Recommended for running lusidtools locally)

```python
Api_factory =  ApiClientFactory(api_secrets_filename="c:full/path/to/secrets.json")
```

###### Using credentials set as Environment Variables 

```python
Api_factory =  ApiClientFactory()
```

###### Using Bearer Token (recommended for running lusidtools from an authenticated browser eg. hosted jupyterhub instance)

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

Extract from DataFrame:

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

The cash_flag mapping field is used by `identify_cash_items()` to identify cash items and either remove them (when loading instruments from data in a transactions file), or mark them as currencies (for loading cash transactions to a portfolio).

The cash items in this extract can be found by inspecting the values contained within 3 columns: inst_name, FIGI_id and Security type. If any of these values match the values specified in the cash_flag config, then that item can be identified as cash.

The example below shows how `identify_cash_items()` uses the cash_flag mapping to perform its two functions:

- removing cash from a dataframe
- Labelling items in a dataframe as Currencies 

| inst_name      | FIGI_id      | Ticker  | Market Sector | Currency | Security type |
| -------------- | ------------ | ------- | ------------- | -------- | ------------- |
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  |
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  |
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  |
|                | Cash USD     | USD     |               | USD      | Cash          |
| JPY            |              | JPY     |               | JPY      | Currency      |

```python
mapping = {
    "instruments": {
        "required": {"name": "inst_name"},
        "identifier_mapping": {"Figi": "FIGI"},
    },
    "cash_flag": {
        "cash_identifiers": {
        	# if any of these criteria are satisfied, then item is cash
            "Security type" : ["Cash", "Currency"],
            "FIGI_id": ["Cash USD", "Cash JPY"],
            "inst_name": ["USD", "JPY"]
        },
        # implicitly infer the currency of the cash from this column
        "implicit": "Currency"
    }
}
```

>  Removing cash from a dataframe

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

>  Labelling cash in a dataframe 

Setting `remove_cash_items` to False labels creates a new column `__currency_identifier_for_LUSID` which is assigned a Currency identifier for any cash items. The mapping for this new field is then appended to the identifier mapping. 

```python
dataframe_cash_labled, mapping_labled = identify_cash_items(
    dataframe = dataframe.copy(), 
    mappings = mapping, 
    file_type = "instruments", 
    remove_cash_items = False
)

# print mapping_labled to show the new identifier_mapping
print(mapping_labled)
>>>{
    "instruments": {
        "required": {"name": "inst_name"},
        "identifier_mapping": {
            "Figi": "FIGI",
            "Currency": "__currency_identifier_for_LUSID"
        },
    },
    "cash_flag": {
        "cash_identifiers": {
        	# if any of these criteria are satisfied, then item is cash
            "Security type" : ["Cash", "Currency"],
            "FIGI_id": ["Cash USD", "Cash JPY"],
            "inst_name": ["USD", "JPY"]
        },
        # implicitly infer the currency of the cash from this column
        "implicit": "Currency"
    }
}

dataframe_cash_labled.head()
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
