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

# 