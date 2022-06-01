import logging
import lusid
import pathlib
from lusidtools.lpt import stdargs
from lusidtools.logger import LusidLogger


def parse(extend=None, args=None):
    return (
        stdargs.Parser(
            "Flush Transactions", ["scope", "optional_portfolio", "date_range", "group", "flush_scope"],
        )
        .extend(extend)
        .parse(args)
    )


def transaction_batcher_by_character_count(
    scope: str, code: str, host: str, input_lst: list, maxCharacterCount: int = 4000
):
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
        f"Batched the transactions into {len(batches)} batches based on the character limit of {maxCharacterCount}"
    )

    return batches


class TxnGetter:
    def __init__(
        self, scope, portfolio, start_date, end_date, transaction_portfolios_api
    ):
        self.scope = scope
        self.portfolio = portfolio
        self.start_date = start_date
        self.end_date = end_date
        self.transaction_portfolios_api = transaction_portfolios_api
        self.stop = False

    def _get_transactions(self, scope, portfolio, start_date, end_date, page=None):
        # make API call to LUSID
        if page is None:
            return self.transaction_portfolios_api.get_transactions(
                scope,
                portfolio,
                from_transaction_date=start_date,
                to_transaction_date=end_date,
                limit=5000,
            )
        else:
            return self.transaction_portfolios_api.get_transactions(
                scope,
                portfolio,
                from_transaction_date=start_date,
                to_transaction_date=end_date,
                limit=5000,
                page=page,
            )

    def __iter__(self):
        # get first page and get next page token
        self.txns = self._get_transactions(
            self.scope, self.portfolio, self.start_date, self.end_date
        )
        self.next_page = self.txns.next_page
        return self

    def __next__(self):
        # if there is a next page token get the next page
        if self.stop:
            raise StopIteration

        result = self.txns
        if self.next_page:
            self.txns = self._get_transactions(
                self.scope,
                self.portfolio,
                self.start_date,
                self.end_date,
                self.next_page,
            )
            self.next_page = self.txns.next_page
            return result
        else:
            self.stop = True
            return result


def get_paginated_txns(
    scope, portfolio, start_date, end_date, transaction_portfolios_api
):
    """
    Gets paginated transactions in a given time window

    Parameters
    ----------
    scope : str
        Scope of the portfolio in question
    portfolio : str
        Code of the portfolio in question
    start_date : str
        Lower bound from when transactions will be looked for
    end_date : str
        Upper bound to when transactions will be looked for
    transaction_portfolios_api : TransactionPortfoliosApi
        The instance of the API used for the requests

    Returns
    -------
    List of all responses from LUSID, each response relating to a page of transactions

    """
    txn_getter = TxnGetter(
        scope, portfolio, start_date, end_date, transaction_portfolios_api
    )
    return [response for response in txn_getter]


def get_portfolios_from_group(expanded_group):
    """
    Recursive function to extract all the portfolio information from the get_portfolio_group_expansion endpoint

    Parameters
    ----------
    expanded_group : ExpandedGroup
        Response from get_portfolio_group_expansion endpoint containing all the portfolio scopes and codes in this group
        as well as further ExpandedGroup objects for each of the sub-groups

    Returns
    -------
    A list of tuples with each item describing a portfolios in the form (scope, code)
    """
    temp_portfolio_lst = [
        (portfolio.id.scope, portfolio.id.code) for portfolio in expanded_group.values
    ]

    if expanded_group.sub_groups:
        for sub_group in expanded_group.sub_groups:
            temp_portfolio_lst.extend(get_portfolios_from_group(sub_group))

    return temp_portfolio_lst


def get_all_transactions_from_portfolio_list(args, portfolio_list, transaction_portfolios_api):
    """
    Gets all transactions from the list of portfolio scope and codes that are passed in. Pagination is handled.

    Parameters
    ----------
    args : Namespace
            The arguments taken in when command is run
    portfolio_list : [tuple(str,str)]
            A list of tuples containing portfolio scope and codes
    transaction_portfolios_api : lusid.TransactionPortfoliosApi
            An instantiated TransactionPortfoliosApi

    Returns
    -------
    txn_dict : dict
        Dictionary where the keys are tuples of portfolio scope and codes and the values are a list of transactions

    """
    # Get transactions from these portfolios
    return {
        (portfolio_scope, portfolio_code): [
            transaction
            for page
            in get_paginated_txns(
                portfolio_scope,
                portfolio_code,
                args.start_date,
                args.end_date,
                transaction_portfolios_api,
            )
            for transaction
            in page.values]
        for (portfolio_scope, portfolio_code)
        in portfolio_list
    }


def get_all_txns(args):
    """
    Gets all the transactions in a given time window

    Parameters
    ----------
    args : Namespace
            The arguments taken in when command is run

    Returns
    -------
    Dictionary with key: (scope, code) and value: list of transactions
    """

    api_factory = lusid.utilities.ApiClientFactory(api_secrets_filename=args.secrets)
    transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)

    if args.group:
        # Get a list of all portfolios from the group
        groups_api = api_factory.build(lusid.api.PortfolioGroupsApi)

        portfolios_response = groups_api.get_portfolio_group_expansion(
            scope=args.scope, code=args.portfolio
        )
        portfolio_lst = get_portfolios_from_group(portfolios_response)

        return get_all_transactions_from_portfolio_list(args, portfolio_lst, transaction_portfolios_api)

    elif args.flush_scope:
        # Get a list of all portfolios in the scope
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        portfolio_lst = [
            (portfolio.id.scope, portfolio.id.code) for
            portfolio in
            portfolios_api.list_portfolios_for_scope(scope=args.scope).values
        ]

        return get_all_transactions_from_portfolio_list(args, portfolio_lst, transaction_portfolios_api)

    else:
        # Return the portfolio and scope passed in
        return {
            (args.scope, args.portfolio): [
                transaction
                for page in get_paginated_txns(
                    args.scope,
                    args.portfolio,
                    args.start_date,
                    args.end_date,
                    transaction_portfolios_api,
                )
                for transaction in page.values
            ]
        }


def flush(args):
    """
        Cancels all transactions found in a given date range for a portfolio, portfolio group or scope

        Parameters
        ----------
        args : Namespace
            The arguments taken in when command is run

        Returns
        ----------
        Number of successful batches : int
        Number of failed batches : int
    """

    # Initialise the api
    api_factory = lusid.utilities.ApiClientFactory(api_secrets_filename=args.secrets)
    transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)

    # Get the Transaction Data
    total_batch_count = 0
    total_failed_batch_count = 0

    for portfolio_id, transactions in get_all_txns(args).items():
        txn_ids = [txn.transaction_id for txn in transactions]

        logging.info(
            f"Looking at portfolio with scope: {portfolio_id[0]} and code: {portfolio_id[1]}"
        )

        if not txn_ids:
            logging.error("No transactions found between the specified dates")
            continue

        logging.info(
            f"Found {len(txn_ids)} transactions in date range {args.end_date} {args.start_date} "
        )

        # Batch the transactions - Necessary to limit the length of the URI in the API call
        txn_ids_batches = transaction_batcher_by_character_count(
            args.scope,
            args.portfolio,
            api_factory.api_client.configuration.host,
            txn_ids,
        )

        # Flush the transactions
        logging.info("Flushing the transactions")
        local_failure_count = 0
        for batch in txn_ids_batches:
            try:
                transaction_portfolios_api.cancel_transactions(
                    portfolio_id[0], portfolio_id[1], transaction_ids=batch
                )
            except lusid.exceptions.ApiException as e:
                logging.error(
                    f"Batch {txn_ids_batches.index(batch)} in portfolio with scope: {portfolio_id[0]} "
                    f"and code: {portfolio_id[1]} has failed with exception {e} "
                )
                local_failure_count = local_failure_count + 1

        total_failed_batch_count = total_failed_batch_count + local_failure_count
        total_batch_count = total_batch_count + len(txn_ids_batches)

        logging.info(
            f"Done flushing this portfolio with {len(txn_ids_batches) - local_failure_count} successes and {local_failure_count} failures"
        )

    return (total_batch_count - total_failed_batch_count), total_failed_batch_count


def main():
    args = parse()
    if args.debug:
        LusidLogger(args.debug)
    successful_batch_count, failed_batch_count = flush(args)

    if failed_batch_count == 0:
        logging.info("All transaction batches were successfully flushed")
    else:
        logging.info(
            f"The following number of batches were successful: {successful_batch_count}"
        )
        logging.error(
            f"The following number of batches failed to flush: {failed_batch_count}"
        )

    return 0


if __name__ == "__main__":
    main()
