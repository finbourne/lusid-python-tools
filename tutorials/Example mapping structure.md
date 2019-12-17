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

