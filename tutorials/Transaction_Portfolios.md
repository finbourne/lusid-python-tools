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

