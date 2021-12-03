import logging
import lusid
import pathlib
from lusidtools.lpt import stdargs


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Flush Transactions",
            [
                "scope",
                "portfolio",
                "date_range",
            ],
        )
            .extend(extend)
            .parse(args)
    )


def transaction_batcher_by_character_count(scope: str, code: str, host: str, input_lst: list,
                                           maxCharacterCount: int = 4000):
    """
    Takes a given input list of transactions or transactionIDs and batches them based on a maximum number of
    characters allowed in the URI

    Parameters
    ----------
    scope : str
        Scope of the portfolio in question
    code : str
        Code of the portfolio in question
    host : str
        Host taken from the configuration of the api client. Represents the start the base url involved in the calls
    input_lst : list
        A list containing all the transaction information
    maxCharacterCount : int
        Maximum number of characters allowed in the URI

    Returns
    -------
    batched_data : list of lists
        Returns a list with each item being a batch (which is itself, a list of transactions)

    """

    # Determine how many characters are fixed to be included in url before the addition of query params
    base_url = f"{host}/api/transactionportfolios/{scope}/{code}/transactions?"
    unavoidable_character_count = len(base_url)
    remaining_count = maxCharacterCount - unavoidable_character_count
    queryLengthPerTxnID = [len(f"transactionIds={x}&") for x in input_lst]

    # Build the batches
    batches = []
    curr_count = 0
    curr_batch = []
    for txn_id, str_len in zip(input_lst, queryLengthPerTxnID):
        if curr_count + str_len >= remaining_count:
            batches.append(curr_batch)
            curr_batch = []
            curr_count = 0

        curr_batch.append(txn_id)
        curr_count = curr_count + str_len

    # Create remainder final batch if any remainder exists
    if curr_batch:
        batches.append(curr_batch)

    return batches


def get_ids_from_txns(txns: lusid.VersionedResourceListOfTransaction):
    """
        Takes a given input list of transactions and extracts the transaction IDs from them

        Parameters
        ----------
        txns : lusid.VersionedResourceListOfTransaction
            Unedited list of transactions from GetTransactions response

        Returns
        -------
        txnIDs : list
            Returns a list of the transaction IDs for the observed transactions
    """
    return [txn.transaction_id for txn in txns.values]


def flush(scope, code, fromDate, toDate, api_factory):
    """
            Cancels all transactions found in a given date range for a specific portfolio

            Parameters
            ----------
            scope : str
                Scope of the portfolio in question
            code : str
                Code of the portfolio in question
            fromDate : str
                Date after which transactions will be retrieved
            toDate : str
                Date up to which transactions will be retrieved
        """
    # Initialise the api
    secrets_path = str(pathlib.Path(__file__).parent.parent.parent.joinpath('tests', "secrets.json"))

    transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)

    # Get the Transaction Data
    print("Getting Transaction Data")
    transactions = transaction_portfolios_api.get_transactions(scope,
                                                               code,
                                                               from_transaction_date=fromDate,
                                                               to_transaction_date=toDate,
                                                               limit=5000)

    txn_ids = get_ids_from_txns(transactions)

    # Manage pagination of GetTransactions
    next_transaction_page = transactions.next_page
    while next_transaction_page is not None:
        page_of_transactions = transaction_portfolios_api.get_transactions(scope,
                                                                           code,
                                                                           from_transaction_date=fromDate,
                                                                           to_transaction_date=toDate,
                                                                           limit=5000,
                                                                           page=next_transaction_page)

        txn_ids = txn_ids + get_ids_from_txns(page_of_transactions)

        next_transaction_page = page_of_transactions.next_page

    if not txn_ids:
        print("No transactions found between the specified dates")
        logging.error("No transactions found between the specified dates")
        return

    logging.info(f"Found {len(txn_ids)} transactions between the specified dates")

    # Batch the transactions
    character_count = 4000
    txn_ids = transaction_batcher_by_character_count(scope, code, api_factory.api_client.configuration.host, txn_ids,
                                                     character_count)
    logging.info(
        f"Batched the transactions into {len(txn_ids)} batches based on the character limit of {character_count}")

    print("Flushing the transactions")

    for batch in txn_ids:
        response = transaction_portfolios_api.cancel_transactions(scope, code, transaction_ids=batch)


def main():
    args = parse()
    print(args)
    api_factory = lusid.utilities.ApiClientFactory(
        api_secrets_filename=args.secrets
    )
    flush(scope=args.scope, code=args.portfolio, fromDate=args.start_date, toDate=args.end_date, api_factory=api_factory)
    return 0


if __name__ == "__main__":
    main()
