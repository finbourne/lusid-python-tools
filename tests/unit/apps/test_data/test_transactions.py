import datetime
from dateutil.tz import tzutc


def gen_transaction_data(txn_count):
    return [{'counterpartyId': None,
             "instrumentIdentifiers": {"Instrument/default/LusidInstrumentId": "CCY_USD"},
             'settlementDate': datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc()),
             'totalConsideration': {'amount': 10, 'currency': 'USD'},
             'transactionCurrency': 'USD',
             'transactionDate': datetime.datetime(2020, 2, 14, 0, 0, tzinfo=tzutc()),
             'transactionId': str(x) + "-test-transactions",
             'transactionPrice': {'price': 1.0, 'type': 'Price'},
             'type': 'FundsIn',
             'units': 10} for x in range(txn_count)]
