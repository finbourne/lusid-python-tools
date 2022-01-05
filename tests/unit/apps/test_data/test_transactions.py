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


group_portfolio_requests = {
    "test_flush_groups_sub_scope": [
        {
            "code": "testFlushSubGroup",
            "created": "2019-10-04T00:00:00.0000000+00:00",
            "values": [
                {
                    "scope": "test_flush_groups_sub_scope",
                    "code": "TestFlushPortfolio02",
                },
                {
                    "scope": "test_flush_groups_sub_scope",
                    "code": "TestFlushPortfolio03",
                },
            ],
            "displayName": "TestFlushSubGroup",
        },
        {
            "code": "testFlushGroupsClean",
            "created": "2019-10-04T00:00:00.0000000+00:00",
            "values": [
                {"scope": "test_flush_groups_sub_scope", "code": "TestFlushPortfolio01"}
            ],
            "subGroups": [
                {"scope": "test_flush_groups_sub_scope", "code": "testFlushSubGroup"}
            ],
            "displayName": "TestFlushGroupClean",
        },
    ],
    "test_flush_groups_scope_01": [
        {
            "code": "testFlushGroupsCleanNoSub",
            "created": "2019-10-04T00:00:00.0000000+00:00",
            "values": [
                {"scope": "test_flush_groups_scope_01", "code": "TestFlushPortfolio01"},
                {"scope": "test_flush_groups_scope_01", "code": "TestFlushPortfolio02"},
                {"scope": "test_flush_groups_scope_01", "code": "TestFlushPortfolio03"},
            ],
            "displayName": "TestFlushGroupCleanNoSub",
        },
        {
            "code": "testFlushGroupsMixedScopes",
            "created": "2019-10-04T00:00:00.0000000+00:00",
            "values": [
                {"scope": "test_flush_groups_scope_02", "code": "TestFlushPortfolio01"},
                {"scope": "test_flush_groups_scope_02", "code": "TestFlushPortfolio02"},
                {"scope": "test_flush_groups_scope_03", "code": "TestFlushPortfolio01"},
                {"scope": "test_flush_groups_scope_03", "code": "TestFlushPortfolio02"},
            ],
            "displayName": "TestFlushGroupMixed",
        },
        {
            "code": "testFlushGroupsSingle",
            "created": "2019-10-04T00:00:00.0000000+00:00",
            "values": [
                {"scope": "test_flush_groups_scope_02", "code": "TestFlushPortfolio03"}
            ],
            "displayName": "TestFlushGroupSingle",
        },
        {
            "code": "testFlushGroupsEmpty",
            "created": "2019-10-04T00:00:00.0000000+00:00",
            "values": [],
            "displayName": "TestFlushGroupEmpty",
        },
    ],
}
