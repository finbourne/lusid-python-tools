from lusidtools.cocoon.async_tools import run_in_executor, ThreadPool
import lusid
import asyncio
from functools import reduce
from typing import Dict, List
from lusidtools.cocoon.async_tools import (
    start_event_loop_new_thread,
    stop_event_loop_new_thread,
)


def _join_holdings(
    holdings_to_join: Dict[str, List[lusid.models.PortfolioHolding]],
    group_by_portfolio: bool = False,
    dict_key="GroupHoldings",
) -> Dict[str, List[lusid.models.PortfolioHolding]]:
    """
    This function joins the holdings together from multiple Portfolios into a single list of PortfolioHolding

    Currently the following constraints exist:

    1) Only support for instrument properties where the first one is taken
    2) Sub-holding-keys are ignored
    3) The portfolio currency is assumed to be the same across all Portfolios

    Parameters
    ----------
    holdings_to_join : Dict[str, List[lusid.models.PortfolioHolding]
        The dictionary of lists of PortfolioHolding keyed by the unique combination of Scope and Code
        for the Portfolio Group or Portfolios. These will either be returned as is or joined to form a new dictionary
        with a single key against a single list of PortfolioHolding.

    group_by_portfolio : bool
        Whether or not to group the holdings by portfolio

    dict_key : str
        The key to use for the merged holdings, usually the scope/code combination of the Portfolio Group

    Returns
    -------
    holdings_joined : Dict[str, List[lusid.models.PortfolioHolding]
        The joined dictionary of a list of PortfolioHolding which either has a single key for the Portfolio
        Group or a key for each Portfolio in the group
    """

    # If each portfolio should remain separated
    if group_by_portfolio:
        return holdings_to_join

    # Flatten the holdings into a single list
    all_holdings = [
        holding
        for holding_list in holdings_to_join.values()
        for holding in holding_list
    ]
    # Initialise a dictionary to hold the positions against a key
    all_holdings_keyed = {}
    # Construct the keyed dictionary using the LUSID instrument id, holding type and currency as the unique key for each holding
    [
        all_holdings_keyed.setdefault(
            f"{holding.instrument_uid}:{holding.holding_type}:{holding.cost.currency}",
            [],
        ).append(holding)
        for holding in all_holdings
    ]

    # Initialise a list to hold the joined holdings
    joined_holdings = []

    # Reduce the list of holdings against each key to a single holding
    for key, value in all_holdings_keyed.items():
        joined_holdings.append(
            lusid.models.PortfolioHolding(
                # Use the instrument_uid from the key
                instrument_uid=key.split(":")[0],
                # Use the holding type from the key
                holding_type=key.split(":")[1],
                # Add the metric fields together
                units=reduce((lambda x, y: x + y), list(map(lambda x: x.units, value))),
                settled_units=reduce(
                    (lambda x, y: x + y), list(map(lambda x: x.settled_units, value))
                ),
                cost=lusid.models.CurrencyAndAmount(
                    # Use the currency form the key
                    currency=key.split(":")[2],
                    amount=reduce(
                        (lambda x, y: x + y), list(map(lambda x: x.cost.amount, value))
                    ),
                ),
                cost_portfolio_ccy=lusid.models.CurrencyAndAmount(
                    # Use the first currency, these holdings could be from different portfolios, so the validity of this is questionable
                    currency=value[0].cost_portfolio_ccy.currency,
                    amount=reduce(
                        (lambda x, y: x + y),
                        list(map(lambda x: x.cost_portfolio_ccy.amount, value)),
                    ),
                ),
                # Takes the properties from the first value, only allows instrument properties
                properties={
                    property_key: value
                    for property_key, value in value[0].properties.items()
                    if value.key.split("/")[0] == "Instrument"
                },
            )
        )

    return {dict_key: joined_holdings}


@run_in_executor
def _get_portfolio_group(
    api_factory: lusid.utilities.ApiClientFactory, scope: str, code: str, **kwargs
) -> lusid.models.PortfolioGroup:
    """
    This function gets a Portfolio Group from LUSID.

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The api factory to use
    scope : str
        The scope of the Portfolio Group
    code : str
        The code of the Portfolio Group, with the scope this uniquely identifiers the Portfolio Group

    Returns
    -------
    response : lusid.models.PortfolioGroup
        The Portfolio Group

    Other Parameters
    ------
    effective_at : datetime
        The effective datetime at which to get the Portfolio Group
    as_at : datetime
        The as at datetime at which to get the Portfolio Group
    thread_pool
        The thread pool to run this function in
    """

    # Filter out the relevant keyword arguments as the LUSID API will raise an exception if given extras
    lusid_keyword_arguments = {
        key: value for key, value in kwargs.items() if key in ["effective_at", "as_at"]
    }

    # Call LUSID to get the portfolio group
    response = lusid.api.PortfolioGroupsApi(
        api_factory.build(lusid.api.PortfolioGroupsApi)
    ).get_portfolio_group(scope=scope, code=code, **lusid_keyword_arguments)

    return response


@run_in_executor
def _get_portfolio_holdings(
    api_factory: lusid.utilities.ApiClientFactory, scope: str, code: str, **kwargs
) -> Dict[str, List[lusid.models.PortfolioHolding]]:
    """
    This function gets the holdings of a Portfolio from LUSID.

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The api factory to use
    scope : str
        The scope of the Portfolio
    code : str
        The code of the Portfolio, with the scope this uniquely identifiers the Portfolio
    effective_at : datetime
        The effective datetime at which to get the Portfolio Group
    as_at : datetime
        The as at datetime at which to get the Portfolio Group
    filter : str
    by_taxlots : bool
    property_keys : list[str]

    Returns
    -------
    response : Dict[str, List[lusid.models.PortfolioHolding]]
        The list of PortfolioHolding keyed by the unique combination of the Portfolio's scope and code

    Other Parameters
    ------
    effective_at : datetime
        The effective datetime at which to get the Portfolio Group
    as_at : datetime
        The as at datetime at which to get the Portfolio Group
    filter : str
        The filter to use to filter the holdings
    by_taxlots : bool
        Whether or not to break the holdings down into individual tax lots
    property_keys : list[str]
        The list of property keys to decorate onto the holdings, must be from the Instrument domain
    thread_pool
        The thread pool to run this function in
    """

    # Filter out the relevant keyword arguments as the LUSID API will raise an exception if given extras
    lusid_keyword_arguments = {
        key: value
        for key, value in kwargs.items()
        if key in ["effective_at", "as_at", "filter", "by_taxlots", "property_keys"]
    }

    # Call LUSID to get the holdings for the Portfolio
    response = lusid.api.TransactionPortfoliosApi(
        api_factory.build(lusid.api.TransactionPortfoliosApi)
    ).get_holdings(scope=scope, code=code, **lusid_keyword_arguments)

    # Key the response with the unique scope/code combination
    return {f"{scope} : {code}": response.values}


async def _get_holdings_for_group_recursive(
    api_factory: lusid.utilities.ApiClientFactory,
    group_scope: str,
    group_code: str,
    group_by_portfolio=False,
    **kwargs,
) -> Dict[str, List[lusid.models.PortfolioHolding]]:
    """
    This function recursively gets the holdings for a Portfolio Group in LUSID by making a request to get the holdings for
    each sub-group and portofolio and then joining the results together above the API.

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The api factory to use
    group_scope : str
        The scope of the Portfolio Group
    group_code : str
        The code of the Portfolio Group
    group_by_portfolio : bool
        Whether or not to group the holdings by Portfolio, if False will merge all Holdings together based on Instrument

    Returns
    -------
    Dict[str, List[lusid.models.PortfolioHolding]]
        The single set of holdings

    Other Parameters
    ------
    effective_at : datetime
        The effective datetime at which to get the Portfolio Group
    as_at : datetime
        The as at datetime at which to get the Portfolio Group
    filter : str
        The filter to use to filter the holdings
    by_taxlots : bool
        Whether or not to break the holdings down into individual tax lots
    property_keys : list[str]
        The list of property keys to decorate onto the holdings, must be from the Instrument domain
    thread_pool
        The thread pool to run this function in
    """

    # Get the details for the Portfolio Group including its sub-group and Portfolio members
    response = await _get_portfolio_group(
        api_factory, group_scope, group_code, **kwargs
    )
    portfolios = response.portfolios
    sub_groups = response.sub_groups

    # Get the holdings for each portfolio
    portfolio_holdings = await asyncio.gather(
        *[
            _get_portfolio_holdings(
                api_factory=api_factory,
                scope=portfolio.scope,
                code=portfolio.code,
                **kwargs,
            )
            for portfolio in portfolios
        ],
        return_exceptions=False,
    )

    # Turn the list of dictionaries into a single dictionary where the holdings are keyed by the Portfolio scope : code
    portfolio_holdings_keyed = {k: v for d in portfolio_holdings for k, v in d.items()}

    # Join the holdings across all portfolios together
    joined_portfolio_holdings = _join_holdings(
        portfolio_holdings_keyed,
        group_by_portfolio,
        dict_key=f"{group_scope} : {group_code}",
    )

    # If there aren't any sub-groups return these joined holdings
    if len(sub_groups) == 0:
        return joined_portfolio_holdings
    # Otherwise get the joined holdings for the sub-groups
    else:
        sub_group_holdings = await asyncio.gather(
            *[
                _get_holdings_for_group_recursive(
                    api_factory=api_factory,
                    group_scope=sub_group.scope,
                    group_code=sub_group.code,
                    group_by_portfolio=group_by_portfolio,
                    **kwargs,
                )
                for sub_group in sub_groups
            ],
            return_exceptions=False,
        )

        # Turn the list of dictionaries into a single dictionary where the holdings are keyed by the Portfolio scope : code
        sub_group_holdings_keyed = {
            k: v for d in sub_group_holdings for k, v in d.items()
        }

        # Join together the holdings across all the sub-groups
        joined_sub_group_holdings = _join_holdings(
            sub_group_holdings_keyed, group_by_portfolio, dict_key="SubGroupHoldings"
        )

        # There shouldn't be any overlap between the keys in these two sets
        if (
            len(
                set(joined_sub_group_holdings.keys()).intersection(
                    set(joined_portfolio_holdings)
                )
            )
            > 0
        ):
            raise ValueError(
                "There are duplicate Portfolios in the Portfolio Group. This is not currently supported. "
                "Please ensure that there are no duplicates in your Porfolio Group via sub-groups and try again"
            )

        # Join and return the joined sub-group holdings with the joined portfolio holdings
        return _join_holdings(
            {**joined_sub_group_holdings, **joined_portfolio_holdings},
            group_by_portfolio,
            dict_key=f"{group_scope} : {group_code}",
        )


def get_holdings_for_group(
    api_factory: lusid.utilities.ApiClientFactory,
    group_scope: str,
    group_code: str,
    group_by_portfolio: bool = False,
    num_threads=5,
    **kwargs,
) -> Dict[str, List[lusid.models.PortfolioHolding]]:
    """
    This function gets the holdings for a Portfolio Group in LUSID.

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The api factory to use
    group_scope : str
        The scope of the Portfolio Group
    group_code : str
        The code of the Portfolio Group
    group_by_portfolio : bool
        Whether or not to group the holdings by Portfolio, if False will merge all Holdings together based on Instrument
    num_threads : int
        The number of threads to use for asynchronous programming

    Returns
    -------
    group_holdings : Dict[str, List[lusid.models.PortfolioHolding]]
        The single set of holdings either keyed by the Portfolio scope/code or the Portfolio Group scope/code

    Other Parameters
    ------
    effective_at : datetime
        The effective datetime at which to get the Portfolio Group
    as_at : datetime
        The as at datetime at which to get the Portfolio Group
    filter : str
        The filter to use to filter the holdings
    by_taxlots : bool
        Whether or not to break the holdings down into individual tax lots
    property_keys : list[str]
        The list of property keys to decorate onto the holdings, must be from the Instrument domain
    """

    # Create a new thread pool to run the asynchronous tasks in
    thread_pool = ThreadPool(num_threads).thread_pool
    kwargs["thread_pool"] = thread_pool

    # Start a new event loop in a new thread, this is required to run inside a Jupyter notebook
    loop = start_event_loop_new_thread()

    # Get the responses from LUSID
    group_holdings = asyncio.run_coroutine_threadsafe(
        _get_holdings_for_group_recursive(
            api_factory=api_factory,
            group_scope=group_scope,
            group_code=group_code,
            group_by_portfolio=group_by_portfolio,
            **kwargs,
        ),
        loop,
    ).result()

    # Stop the additional event loop
    stop_event_loop_new_thread(loop)

    return group_holdings
