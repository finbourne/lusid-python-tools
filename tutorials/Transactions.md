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

