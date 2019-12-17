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

