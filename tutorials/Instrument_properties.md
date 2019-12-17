# Instrument properties

Additional instrument properties can be applied to the instruments. An example of this is **strategy tagging**.

| inst_name      | FIGI         | Ticker  | Market Sector | Currency | Security type | Strategy |
| -------------- | ------------ | ------- | ------------- | -------- | ------------- | -------- |
| Apple          | BBG0013T5HY0 | APPLE   | M-Mkt         | USD      | Common Stock  | Evil     |
| MICROSOFT CORP | BBG007F5XJZ0 | MSFTCHF | Corp          | USD      | Common Stock  | Good     |
| UBER Tech      | BBG00NW4HSM1 | UBER    | Equity        | USD      | Common Stock  | Evil     |

	The required mapping for upserting instrument properties can be found in the online API documentation [Upsert Instrument Properties](https://www.lusid.com/docs/api/operation/UpsertInstruments#operation/UpsertInstrumentsProperties) 

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