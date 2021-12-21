import datetime
from dateutil.tz import tzutc


def gen_transaction_data(txn_count, txn_date):
    return [
        {
            "counterpartyId": None,
            "instrumentIdentifiers": {
                "Instrument/default/LusidInstrumentId": "CCY_USD"
            },
            "settlementDate": txn_date,
            "totalConsideration": {"amount": 10, "currency": "USD"},
            "transactionCurrency": "USD",
            "transactionDate": txn_date,
            "transactionId": str(x) + "-test-transactions-" + str(txn_date),
            "transactionPrice": {"price": 1.0, "type": "Price"},
            "type": "FundsIn",
            "units": 10,
        }
        for x in range(txn_count)
    ]
