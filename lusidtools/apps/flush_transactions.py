import logging
import lusid
import pathlib
from lusidtools.lpt import stdargs
from lusidtools.logger import LusidLogger


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

    # Build the batches
    batches = []
    curr_count = 0
    curr_batch = []
    for txn_id in input_lst:
        if curr_count + len(f"transactionIds={txn_id}&") >= remaining_count:
            batches.append(curr_batch)
            curr_batch = []
            curr_count = 0

        curr_batch.append(txn_id)
        curr_count = curr_count + len(f"transactionIds={txn_id}&")

    # Create remainder final batch if any remainder exists
    if curr_batch:
        batches.append(curr_batch)

    logging.info(
        f"Batched the transactions into {len(batches)} batches based on the character limit of {maxCharacterCount}")

    return batches


class TxnGetter:

    def _get_transactions(self, args, page=None):
        # make API call to LUSID
        if page is None:
            return self.transaction_portfolios_api.get_transactions(args.scope,
                                                                    args.portfolio,
                                                                    from_transaction_date=args.start_date,
                                                                    to_transaction_date=args.end_date,
                                                                    limit=5000,
                                                                    )
        else:
            return self.transaction_portfolios_api.get_transactions(args.scope,
                                                                    args.portfolio,
                                                                    from_transaction_date=args.start_date,
                                                                    to_transaction_date=args.end_date,
                                                                    limit=5000,
                                                                    page=page
                                                                    )

    def __init__(self, args):
        self.args = args
        api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=args.secrets
        )
        self.transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        self.stop = False

    def __iter__(self):
        # get first page and get next page token
        self.txns = self._get_transactions(self.args)
        self.next_page = self.txns.next_page
        return self

    def __next__(self):
        # if there is a next page token get the next page
        if self.stop:
            raise StopIteration

        result = self.txns
        if self.next_page:
            self.txns = self._get_transactions(self.args, self.next_page)
            self.next_page = self.txns.next_page
            return result
        else:
            self.stop = True
            return result


def get_paginated_txns(args):
    txn_getter = TxnGetter(args)
    return [response for response in txn_getter]


def get_all_txns(args):
    flat_list = []
    # Iterate through the outer list
    for element in get_paginated_txns(args):
        flat_list = flat_list + element.values
    return flat_list


def flush(args):
    """
            Cancels all transactions found in a given date range for a specific portfolio

            Parameters
            ----------
            args : Namespace
                The arguments taken in when command is run
        """

    # Initialise the api
    api_factory = lusid.utilities.ApiClientFactory(
        api_secrets_filename=args.secrets
    )
    transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)

    # Get the Transaction Data
    txn_ids = [txn.transaction_id for txn in get_all_txns(args)]

    if not txn_ids:
        logging.error("No transactions found between the specified dates")
        return

    logging.info(f"Found {len(txn_ids)} transactions in date range {args.end_date} {args.start_date} ")

    # Batch the transactions - Necessary to limit the length of the URI in the API call
    txn_ids_batches = transaction_batcher_by_character_count(args.scope, args.portfolio,
                                                             api_factory.api_client.configuration.host,
                                                             txn_ids
                                                             )

    logging.info("Flushing the transactions")

    for batch in txn_ids_batches:
        response = transaction_portfolios_api.cancel_transactions(args.scope, args.portfolio, transaction_ids=batch)


def main():
    args = parse()
    if args.debug:
        LusidLogger(args.debug)
    flush(args)

    return 0


if __name__ == "__main__":
    main()
