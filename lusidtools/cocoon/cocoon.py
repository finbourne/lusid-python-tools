import asyncio
import lusid
import pandas as pd
import numpy as np
import json
from lusidtools import cocoon
from lusidtools.cocoon.async_tools import run_in_executor, ThreadPool
from lusidtools.cocoon.dateorcutlabel import DateOrCutLabel
from lusidtools.cocoon.utilities import (
    checkargs,
    strip_whitespace,
    group_request_into_one,
)
from lusidtools.cocoon.validator import Validator
from datetime import datetime
import pytz
from lusidtools.logger import LusidLogger
import logging


class BatchLoader:
    """
    This class contains all the methods used for loading data in batches. The @run_in_executor decorator makes the
    synchronous functions awaitable
    """

    @staticmethod
    @run_in_executor
    def load_instrument_batch(
        api_factory: lusid.utilities.ApiClientFactory, instrument_batch: list, **kwargs
    ) -> lusid.models.UpsertInstrumentsResponse:
        """
        Upserts a batch of instruments to LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            The api factory to use
        instrument_batch : list[lusid.models.InstrumentDefinition]
            The batch of instruments to upsert
        **kwargs
            arguments specific to each call e.g. effective_at for holdings, unique_identifiers

        Returns
        -------
        lusid.models.UpsertInstrumentsResponse
            The response from LUSID
        """

        # Ensure that the list of allowed unique identifiers exists
        if "unique_identifiers" not in list(kwargs.keys()):
            unique_identifiers = cocoon.instruments.get_unique_identifiers(
                api_factory=api_factory
            )
        else:
            unique_identifiers = kwargs["unique_identifiers"]

        @checkargs
        def get_alphabetically_first_identifier_key(
            instrument: lusid.models.InstrumentDefinition, unique_identifiers: list
        ):
            """
            Gets the alphabetically first occurring unique identifier on an instrument and use it as the correlation
            id on the request

            Parameters
            ----------
            instrument : lusid.models.InstrumentDefinition
                The instrument to create a correlation id for
            unique_identifiers : list[str] unique_identifiers
                The list of allowed unique identifiers

            Returns
            -------
            str
                The correlation id to use on the request
            """

            unique_identifiers_populated = list(
                set(unique_identifiers).intersection(
                    set(list(instrument.identifiers.keys()))
                )
            )
            unique_identifiers_populated.sort()
            first_unique_identifier_alphabetically = unique_identifiers_populated[0]
            return f"{first_unique_identifier_alphabetically}: {instrument.identifiers[first_unique_identifier_alphabetically].value}"

        return api_factory.build(lusid.api.InstrumentsApi).upsert_instruments(
            request_body={
                get_alphabetically_first_identifier_key(
                    instrument, unique_identifiers
                ): instrument
                for instrument in instrument_batch
            }
        )

    @staticmethod
    @run_in_executor
    def load_quote_batch(
        api_factory: lusid.utilities.ApiClientFactory, quote_batch: list, **kwargs
    ) -> lusid.models.UpsertQuotesResponse:
        """
        Upserts a batch of quotes into LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            The api factory to use
        quote_batch : list[lusid.models.UpsertQuoteRequest]
            The batch of quotes to upsert
        kwargs
            scope

        Returns
        -------
        lusid.models.UpsertQuotesResponse
            The response from LUSID

        """

        if "scope" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load quotes without a scope, please ensure that a scope is provided."
            )

        return api_factory.build(lusid.api.QuotesApi).upsert_quotes(
            scope=kwargs["scope"],
            request_body={
                "_".join(
                    [
                        quote.quote_id.quote_series_id.instrument_id,
                        quote.quote_id.quote_series_id.instrument_id_type,
                        str(quote.quote_id.effective_at),
                    ]
                ): quote
                for quote in quote_batch
            },
        )

    @staticmethod
    @run_in_executor
    def load_transaction_batch(
        api_factory: lusid.utilities.ApiClientFactory, transaction_batch: list, **kwargs
    ) -> lusid.models.UpsertPortfolioTransactionsResponse:
        """
        Upserts a batch of transactions into LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            The api factory to use
        code : str
            The code of the TransactionPortfolio to upsert the transactions into
        transaction_batch : list[lusid.models.TransactionRequest]
            The batch of transactions to upsert
        kwargs
            code -The code of the TransactionPortfolio to upsert the transactions into

        Returns
        -------
        lusid.models.UpsertPortfolioTransactionsResponse
            The response from LUSID
        """

        if "scope" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load transactions without a scope, please ensure that a scope is provided."
            )

        if "code" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load transactions without a portfolio code, please ensure that a code is provided."
            )

        return api_factory.build(
            lusid.api.TransactionPortfoliosApi
        ).upsert_transactions(
            scope=kwargs["scope"],
            code=kwargs["code"],
            transaction_request=transaction_batch,
        )

    @staticmethod
    @run_in_executor
    def load_holding_batch(
        api_factory: lusid.utilities.ApiClientFactory, holding_batch: list, **kwargs
    ) -> lusid.models.HoldingsAdjustment:
        """
        Upserts a batch of holdings into LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            The api factory to use
        holding_batch : list[lusid.models.AdjustHoldingRequest]
            The batch of holdings
        scope : str
            The scope to upsert holdings into
        code : str
            The code of the portfolio to upsert holdings into
        effective_at : str/Datetime/np.datetime64/np.ndarray/pd.Timestamp
            The effective date of the holdings batch
        kwargs

        Returns
        -------
        lusid.models.HoldingsAdjustment
            The response from LUSID

        """

        if "scope" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load transactions without a scope, please ensure that a scope is provided."
            )

        if "code" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load transactions without a portfolio code, please ensure that a code is provided."
            )

        if "effective_at" not in list(kwargs.keys()):
            raise KeyError(
                """There is no mapping for effective_at in the required mapping, please add it"""
            )

        # If only an adjustment has been specified
        if (
            "holdings_adjustment_only" in list(kwargs.keys())
            and kwargs["holdings_adjustment_only"]
        ):
            return api_factory.build(
                lusid.api.TransactionPortfoliosApi
            ).adjust_holdings(
                scope=kwargs["scope"],
                code=kwargs["code"],
                effective_at=str(DateOrCutLabel(kwargs["effective_at"])),
                adjust_holding_request=holding_batch,
            )

        return api_factory.build(lusid.api.TransactionPortfoliosApi).set_holdings(
            scope=kwargs["scope"],
            code=kwargs["code"],
            effective_at=str(DateOrCutLabel(kwargs["effective_at"])),
            adjust_holding_request=holding_batch,
        )

    @staticmethod
    @run_in_executor
    def load_portfolio_batch(
        api_factory: lusid.utilities.ApiClientFactory, portfolio_batch: list, **kwargs
    ) -> lusid.models.Portfolio:
        """
        Upserts a batch of portfolios to LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            the api factory to use
        portfolio_batch : list[lusid.models.CreateTransactionPortfolioRequest]
            The batch of portfolios to create
        scope : str
            The scope to create the portfolios in
        code : str
            The code of the portfolio to create
        kwargs

        Returns
        -------
        lusid.models.Portfolio
            the response from LUSID
        """

        if "scope" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load transactions without a scope, please ensure that a scope is provided."
            )

        if "code" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load transactions without a portfolio code, please ensure that a code is provided."
            )

        try:
            return api_factory.build(lusid.api.PortfoliosApi).get_portfolio(
                scope=kwargs["scope"], code=kwargs["code"]
            )
        # Add in here upsert portfolio properties if it does exist
        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                return api_factory.build(
                    lusid.api.TransactionPortfoliosApi
                ).create_portfolio(
                    scope=kwargs["scope"],
                    create_transaction_portfolio_request=portfolio_batch[0],
                )
            else:
                return e

    @staticmethod
    @run_in_executor
    def load_reference_portfolio_batch(
        api_factory: lusid.utilities.ApiClientFactory,
        reference_portfolio_batch: list,
        **kwargs,
    ) -> lusid.models.Portfolio:
        """
        Upserts a batch of reference portfolios to LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            the api factory to use
        portfolio_batch : list[lusid.models.CreateReferencePortfolioRequest]
            The batch of reference portfolios to create
        scope : str
            The scope to create the reference portfolios in
        code : str
            The code of the reference portfolio to create
        kwargs

        Returns
        -------
        lusid.models.ReferencePortfolio
            the response from LUSID
        """

        if "scope" not in kwargs.keys():
            raise KeyError(
                "You are trying to load a reference portfolio without a scope, please ensure that a scope is provided."
            )

        if "code" not in kwargs.keys():
            raise KeyError(
                "You are trying to load a reference portfolio without a portfolio code, please ensure that a code is provided."
            )

        try:
            return api_factory.build(lusid.api.PortfoliosApi).get_portfolio(
                scope=kwargs["scope"], code=kwargs["code"]
            )
        # TODO: Add in here upsert portfolio properties if it does exist

        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                return api_factory.build(
                    lusid.api.ReferencePortfolioApi
                ).create_reference_portfolio(
                    scope=kwargs["scope"],
                    create_reference_portfolio_request=reference_portfolio_batch[0],
                )
            else:
                return e

    @staticmethod
    @run_in_executor
    def load_instrument_property_batch(
        api_factory: lusid.utilities.ApiClientFactory, property_batch: list, **kwargs
    ) -> [lusid.models.UpsertInstrumentPropertiesResponse]:
        """
        Add properties to the set instruments

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            The api factory to use
        property_batch : list[lusid.models.UpsertInstrumentPropertyRequest]
            Properties to add,
            identifiers will be resolved to a LusidInstrumentId, where an identifier resolves to more
            than one LusidInstrumentId the property will be added to all matching instruments
        kwargs

        Returns
        -------
        list[lusid.models.UpsertInstrumentPropertiesResponse]
            the response from LUSID
        """

        results = []
        for request in property_batch:
            search_request = lusid.models.InstrumentSearchProperty(
                key=f"instrument/default/{request.identifier_type}",
                value=request.identifier,
            )

            # find the matching instruments
            mastered_instruments = api_factory.build(
                lusid.api.SearchApi
            ).instruments_search(
                instrument_search_property=[search_request], mastered_only=True
            )

            # flat map the results to a list of luids
            luids = [
                luid
                for luids in [
                    list(
                        map(
                            lambda m: m.identifiers["LusidInstrumentId"].value,
                            mastered.mastered_instruments,
                        )
                    )
                    for mastered in [matches for matches in mastered_instruments]
                ]
                for luid in luids
            ]

            if len(luids) == 0:
                continue

            properties_request = [
                lusid.models.UpsertInstrumentPropertyRequest(
                    identifier_type="LusidInstrumentId",
                    identifier=luid,
                    properties=request.properties,
                )
                for luid in luids
            ]

            results.append(
                api_factory.build(
                    lusid.api.InstrumentsApi
                ).upsert_instruments_properties(properties_request)
            )

        return results

    @staticmethod
    @run_in_executor
    def load_portfolio_group_batch(
        api_factory: lusid.utilities.ApiClientFactory,
        portfolio_group_batch: list,
        **kwargs,
    ) -> lusid.models.PortfolioGroup:
        """
        Upserts a batch of portfolios to LUSID

        Parameters
        ----------
        api_factory : lusid.utilities.ApiClientFactory
            the api factory to use
        portfolio_group_batch : list[lusid.models.CreateTransactionPortfolioRequest]
            The batch of portfilios to create
        scope : str
            The scope to create the portfolio group in
        code : str
            The code of the portfolio group to create
        kwargs

        Returns
        -------
        lusid.models.PortfolioGroup
            The response from LUSID
        """

        updated_request = group_request_into_one(
            portfolio_group_batch[0].__class__.__name__,
            portfolio_group_batch,
            ["values"],
        )

        if "scope" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load a portfolio group without a scope, please ensure that a scope is provided."
            )

        if "code" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load a portfolio group without a portfolio code, please ensure that a code is provided."
            )

        try:

            current_portfolio_group = api_factory.build(
                lusid.api.PortfolioGroupsApi
            ).get_portfolio_group(scope=kwargs["scope"], code=kwargs["code"])

            #  Capture all portfolios - the ones currently in group + the new ones to be added
            all_portfolios_to_add = (
                updated_request.values + current_portfolio_group.portfolios
            )

            current_portfolios_in_group = [
                code
                for code in updated_request.values
                if code in current_portfolio_group.portfolios
            ]

            if len(current_portfolios_in_group) > 0:
                for code in current_portfolios_in_group:
                    logging.info(
                        f"The portfolio {code.code} with scope {code.scope} is already in group {current_portfolio_group.id.code}"
                    )

            # Parse out new portfolios only
            new_portfolios = [
                code
                for code in all_portfolios_to_add
                if code not in current_portfolio_group.portfolios
            ]

            for code, scope in set(
                [(resource.code, resource.scope) for resource in new_portfolios]
            ):

                try:

                    current_portfolio_group = api_factory.build(
                        lusid.api.PortfolioGroupsApi
                    ).add_portfolio_to_group(
                        scope=kwargs["scope"],
                        code=kwargs["code"],
                        effective_at=datetime.now(tz=pytz.UTC),
                        resource_id=lusid.models.ResourceId(scope=scope, code=code),
                    )

                except lusid.exceptions.ApiException as e:
                    logging.error(json.loads(e.body)["title"])

            return current_portfolio_group

        # Add in here upsert portfolio properties if it does exist
        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                return api_factory.build(
                    lusid.api.PortfolioGroupsApi
                ).create_portfolio_group(
                    scope=kwargs["scope"],
                    create_portfolio_group_request=updated_request,
                )
            else:
                return e


async def _load_data(
    api_factory: lusid.utilities.ApiClientFactory,
    single_requests: list,
    file_type: str,
    **kwargs,
):
    """
    This function calls the appropriate batch loader

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The api factory to use
    single_requests
        The list of single requests for LUSID
    file_type : str
        The file type e.g. instruments, portfolios etc.
    kwargs
        arguments specific to each call e.g. effective_at for holdings

    Returns
    -------
    BatchLoader : StaticMethod
        A static method on batchloader
    """

    # Dynamically call the correct async function to use based on the file type
    return await getattr(BatchLoader, f"load_{file_type}_batch")(
        api_factory,
        single_requests,
        # Any specific arguments e.g. 'code' for transactions, 'effective_at' for holdings is passed in via **kwargs
        **kwargs,
    )


def _convert_batch_to_models(
    data_frame: pd.DataFrame,
    mapping_required: dict,
    mapping_optional: dict,
    property_columns: list,
    properties_scope: str,
    instrument_identifier_mapping: dict,
    file_type: str,
    domain_lookup: dict,
    sub_holding_keys: list,
    **kwargs,
):
    """
    This function populates the required models from a DataFrame and loads the data into LUSID

    Parameters
    ----------
    data_frame : pd.DataFrame
        The DataFrame containing the data to load
    mapping_required : dict
        The required mapping
    mapping_optional : dict
        The optional mapping
    property_columns : list
        The property columns to add as property values
    properties_scope : str
        The scope to add the property values in
    instrument_identifier_mapping : dict
        The mapping for the identifiers
    file_type : str
        The file type to load
    domain_lookup : dict
        The domain lookup
    sub_holding_keys : list
        The sub holding keys to use
    kwargs
        Arguments specific to each call e.g. effective_at for holdings

    Returns
    -------
    single_requests : list
         A list of populated LUSID request models
    """

    # Get the data types of the columns to be added as properties
    property_dtypes = data_frame.loc[:, property_columns].dtypes

    # Get the types of the attributes on the top level model for this request
    open_api_types = getattr(
        lusid.models, domain_lookup[file_type]["top_level_model"]
    ).openapi_types

    # If there is a sub_holding_keys attribute and it has a dict type this means the sub_holding_keys
    # need to be populated with property values
    if (
        "sub_holding_keys" in open_api_types.keys()
        and "dict" in open_api_types["sub_holding_keys"]
    ):
        sub_holding_key_dtypes = data_frame.loc[:, sub_holding_keys].dtypes
    # If not and they are provided as full keys
    elif len(sub_holding_keys) > 0:
        sub_holding_keys_row = cocoon.properties._infer_full_property_keys(
            partial_keys=sub_holding_keys,
            properties_scope=properties_scope,
            domain="Transaction",
        )
    # If no keys
    else:
        sub_holding_keys_row = None

    unique_identifiers = kwargs["unique_identifiers"]

    # Iterate over the DataFrame creating the single requests
    single_requests = []
    for index, row in data_frame.iterrows():

        # Create the property values for this row
        if domain_lookup[file_type]["domain"] is None:
            properties = None
        else:
            properties = cocoon.properties.create_property_values(
                row=row,
                scope=properties_scope,
                domain=domain_lookup[file_type]["domain"],
                dtypes=property_dtypes,
            )

        # Create the sub-holding-keys for this row
        if (
            "sub_holding_keys" in open_api_types.keys()
            and "dict" in open_api_types["sub_holding_keys"]
        ):
            sub_holding_keys_row = cocoon.properties.create_property_values(
                row=row,
                scope=properties_scope,
                domain="Transaction",
                dtypes=sub_holding_key_dtypes,
            )

        # Create identifiers for this row if applicable
        if instrument_identifier_mapping is None or not bool(
            instrument_identifier_mapping
        ):
            identifiers = None
        else:
            identifiers = cocoon.instruments.create_identifiers(
                index=index,
                row=row,
                file_type=file_type,
                instrument_identifier_mapping=instrument_identifier_mapping,
                unique_identifiers=unique_identifiers,
                full_key_format=kwargs["full_key_format"],
            )

        # Construct the from the mapping, properties and identifiers the single request object and add it to the list
        single_requests.append(
            cocoon.utilities.populate_model(
                model_object_name=domain_lookup[file_type]["top_level_model"],
                required_mapping=mapping_required,
                optional_mapping=mapping_optional,
                row=row,
                properties=properties,
                identifiers=identifiers,
                sub_holding_keys=sub_holding_keys_row,
            )
        )

    return single_requests


async def _construct_batches(
    api_factory: lusid.utilities.ApiClientFactory,
    data_frame: pd.DataFrame,
    mapping_required: dict,
    mapping_optional: dict,
    property_columns: list,
    properties_scope: str,
    instrument_identifier_mapping: dict,
    batch_size: int,
    file_type: str,
    domain_lookup: dict,
    sub_holding_keys: list,
    **kwargs,
):
    """
    This constructs the batches and asynchronously sends them to be loaded into LUSID

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory
        The api factory to use
    data_frame : pd.DataFrame
        The DataFrame containing the data to load
    mapping_required : dict
        The required mapping
    mapping_optional : dict
        The optional mapping
    property_columns : list
        The property columns to add as property values
    properties_scope : str
        The scope to add the property values in
    instrument_identifier_mapping : dict
        The mapping for the identifiers
    batch_size : int
        The batch size to use
    file_type : str
        The file type to load
    domain_lookup : dict
        The domain lookup
    sub_holding_keys : list
        The sub holding keys to use
    kwargs
        Arguments specific to each call e.g. effective_at for holdings

    Returns
    -------
    dict
        Contains the success responses and the errors (where an API exception has been raised)
    """

    # Get the different behaviours required for different entities e.g quotes can be batched without worrying about portfolios
    batching_no_portfolios = [
        file_type
        for file_type, settings in domain_lookup.items()
        if not settings["portfolio_specific"]
    ]
    batching_with_portfolios = [
        file_type
        for file_type, settings in domain_lookup.items()
        if settings["portfolio_specific"]
    ]

    if file_type in batching_no_portfolios:

        # Everything can be sent up asynchronously, prepare batches based on batch size alone
        async_batches = [
            data_frame.iloc[i : i + batch_size]
            for i in range(0, len(data_frame), batch_size)
        ]

        # Nest the async batches inside a single synchronous batch
        sync_batches = [
            {
                "async_batches": async_batches,
                "codes": [None] * len(async_batches),
                "effective_at": [None] * len(async_batches),
            }
        ]

    elif file_type in batching_with_portfolios:

        if "effective_at" in domain_lookup[file_type]["required_call_attributes"]:

            # Get unique effective dates
            unique_effective_dates = list(
                data_frame[mapping_required["effective_at"]].unique()
            )

            # Create a group for each effective date as they can not be batched asynchronously
            effective_at_groups = [
                data_frame.loc[
                    data_frame[mapping_required["effective_at"]] == effective_at
                ]
                for effective_at in unique_effective_dates
            ]

            # Create a synchronous batch for each effective date
            sync_batches = [
                {
                    # Different portfolio codes can be batched asynchronously inside the synchronous batch
                    "async_batches": [
                        effective_at_group.loc[
                            data_frame[mapping_required["code"]] == code
                        ]
                        for code in list(
                            effective_at_group[mapping_required["code"]].unique()
                        )
                    ],
                    "codes": list(
                        effective_at_group[mapping_required["code"]].unique()
                    ),
                    "effective_at": [
                        list(
                            effective_at_group[
                                mapping_required["effective_at"]
                            ].unique()
                        )[0]
                    ]
                    * len(list(effective_at_group[mapping_required["code"]].unique())),
                }
                for effective_at_group in effective_at_groups
            ]

        else:

            unique_portfolios = list(data_frame[mapping_required["code"]].unique())

            # Different portfolio codes can be batched asynchronously
            async_batches = [
                data_frame.loc[data_frame[mapping_required["code"]] == code]
                for code in unique_portfolios
            ]

            # Inside the synchronous batch split the values for each portfolio into appropriate batch sizes
            sync_batches = [
                {
                    "async_batches": [
                        async_batch.iloc[i : i + batch_size]
                        for async_batch in async_batches
                    ],
                    "codes": [str(code) for code in unique_portfolios],
                    "effective_at": [None] * len(async_batches),
                }
                for i in range(
                    0,
                    max([len(async_batch) for async_batch in async_batches]),
                    batch_size,
                )
            ]

    # Asynchronously load the data into LUSID
    responses = [
        await asyncio.gather(
            *[
                _load_data(
                    api_factory=api_factory,
                    single_requests=_convert_batch_to_models(
                        data_frame=async_batch,
                        mapping_required=mapping_required,
                        mapping_optional=mapping_optional,
                        property_columns=property_columns,
                        properties_scope=properties_scope,
                        instrument_identifier_mapping=instrument_identifier_mapping,
                        file_type=file_type,
                        domain_lookup=domain_lookup,
                        sub_holding_keys=sub_holding_keys,
                        **kwargs,
                    ),
                    file_type=file_type,
                    code=code,
                    effective_at=effective_at,
                    **kwargs,
                )
                for async_batch, code, effective_at in zip(
                    sync_batch["async_batches"],
                    sync_batch["codes"],
                    sync_batch["effective_at"],
                )
                if not async_batch.empty
            ],
            return_exceptions=True,
        )
        for sync_batch in sync_batches
    ]

    responses_flattened = [
        response for responses_sub in responses for response in responses_sub
    ]

    # Raise any internal exceptions rather than propagating them to the response
    for response in responses_flattened:
        if isinstance(response, Exception) and not isinstance(
            response, lusid.exceptions.ApiException
        ):
            raise response

    # Collects the exceptions as failures and successful calls as values
    return {
        "errors": [r for r in responses_flattened if isinstance(r, Exception)],
        "success": [r for r in responses_flattened if not isinstance(r, Exception)],
    }


@checkargs
def load_from_data_frame(
    api_factory: lusid.utilities.ApiClientFactory,
    scope: str,
    data_frame: pd.DataFrame,
    mapping_required: dict,
    mapping_optional: dict,
    file_type: str,
    identifier_mapping: dict = None,
    property_columns: list = None,
    properties_scope: str = None,
    batch_size: int = None,
    remove_white_space: bool = True,
    instrument_name_enrichment: bool = False,
    sub_holding_keys: list = None,
    holdings_adjustment_only: bool = False,
    thread_pool_max_workers: int = 5,
):
    """

    Parameters
    ----------
    api_factory : lusid.utilities.ApiClientFactory api_factory
        The api factory to use
    scope : str
        The scope of the resource to load the data into
    data_frame : pd.DataFrame
        The DataFrame containing the data
    mapping_required : dict{str, str}
        The dictionary mapping the DataFrame columns to LUSID's required attributes
    mapping_optional : dict{str, str}
        The dictionary mapping the DataFrame columns to LUSID's optional attributes
    file_type : str
        The type of file e.g. transactions, instruments, holdings, quotes, portfolios
    identifier_mapping : dict{str, str}
        The dictionary mapping of LUSID instrument identifiers to identifiers in the DataFrame
    property_columns : list
        The columns to create properties for
    properties_scope : str
        The scope to add the properties to
    batch_size : int
        The size of the batch to use when using upsert calls e.g. upsert instruments, upsert quotes etc.
    remove_white_space : bool
        remove whitespace either side of each value in the dataframe
    instrument_name_enrichment : bool
        request additional identifier information from open-figi
    sub_holding_keys : list
        The sub holding keys to use for this request. Can be a list of property keys or a list of
        columns in the dataframe to use to create sub holdings
    holdings_adjustment_only : bool
        Whether to use the adjust_holdings api call rather than set_holdings when working with holdings
    thread_pool_max_workers : int
        The maximum number of workers to use in the thread pool used by the function

    Returns
    -------
    responses: dict
        The responses from loading the data into LUSID

    Examples
    --------

    * Loading Instruments

    .. code-block:: none

        result = lusidtools.cocoon.load_from_data_frame(
            api_factory=api_factory,
            scope=scope,
            data_frame=instr_df,
            mapping_required=mapping["instruments"]["required"],
            mapping_optional={},
            file_type="instruments",
            identifier_mapping=mapping["instruments"]["identifier_mapping"],
            property_columns=mapping["instruments"]["properties"],
            properties_scope=scope
        )

    * Loading Instrument Properties

    .. code-block:: none

        result = lusidtools.cocoon.load_from_data_frame(
            api_factory=api_factory,
            scope=scope,
            data_frame=strat_properties,
            mapping_required=strat_mapping,
            mapping_optional={},
            file_type="instrument_property",
            property_columns=["block tag"],
            properties_scope=scope
        )

    * Loading Portfolios

    .. code-block:: none

        result = lusidtools.cocoon.load_from_data_frame(
            api_factory=api_factory,
            scope=scope,
            data_frame=portfolios,
            mapping_required=mapping["portfolios"]["required"],
            mapping_optional={},
            file_type="portfolios"
        )

    * Loading Transactions

    .. code-block:: none

        result = lusidtools.cocoon.load_from_data_frame(
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


    * Loading Quotes

    .. code-block:: none

        result = lpt.load_from_data_frame(
            api_factory=api_factory,
            scope=scope,
            data_frame=df_adjusted_quotes,
            mapping_required=mapping["quotes"]["required"],
            mapping_optional={},
            file_type="quotes"
        )

    * loading Holdings

    .. code-block:: none

        result = lpt.load_from_data_frame(
            api_factory=api_factory,
            scope=holdings_scope,
            data_frame=seg_df,
            mapping_required=mapping["holdings"]["required"],
            mapping_optional=mapping["holdings"]["optional"],
            identifier_mapping=holdings_mapping["holdings"]["identifier_mapping"],
            file_type="holdings"
        )

    """

    # A mapping between the file type and relevant attributes e.g. domain, top_level_model etc.
    domain_lookup = cocoon.utilities.load_json_file("config/domain_settings.json")

    # Convert the file type to lower case & singular as well as checking it is of the allowed value
    file_type = (
        Validator(file_type, "file_type")
        .make_singular()
        .make_lower()
        .check_allowed_value(list(domain_lookup.keys()))
        .value
    )

    # Ensures that it is a single index dataframe
    Validator(data_frame.index, "data_frame_index").check_is_not_instance(pd.MultiIndex)

    # Set defaults aligned with the data type of each argument, this allows for users to provide None
    identifier_mapping = (
        Validator(identifier_mapping, "identifier_mapping")
        .set_default_value_if_none(default={})
        .discard_dict_keys_none_value()
        .value
    )

    properties_scope = (
        Validator(properties_scope, "properties_scope")
        .set_default_value_if_none(default=scope)
        .value
    )

    property_columns = (
        Validator(property_columns, "property_columns")
        .set_default_value_if_none(default=[])
        .value
    )

    sub_holding_keys = (
        Validator(sub_holding_keys, "sub_holding_keys")
        .set_default_value_if_none(default=[])
        .value
    )

    batch_size = (
        Validator(batch_size, "batch_size")
        .set_default_value_if_none(domain_lookup[file_type]["default_batch_size"])
        .override_value(
            not domain_lookup[file_type]["batch_allowed"],
            domain_lookup[file_type]["default_batch_size"],
        )
        .value
    )

    # Discard mappings where the provided value is None
    mapping_required = (
        Validator(mapping_required, "mapping_required")
        .discard_dict_keys_none_value()
        .value
    )

    mapping_optional = (
        Validator(mapping_optional, "mapping_optional")
        .discard_dict_keys_none_value()
        .value
    )

    required_call_attributes = domain_lookup[file_type]["required_call_attributes"]
    if "scope" in required_call_attributes:
        required_call_attributes.remove("scope")

    # Check that all required parameters exist
    Validator(
        required_call_attributes, "required_attributes_for_call"
    ).check_subset_of_list(list(mapping_required.keys()), "required_mapping")

    # Verify that all the required attributes for this top level model exist in the provided required mapping
    cocoon.utilities.verify_all_required_attributes_mapped(
        mapping=mapping_required,
        model_object_name=domain_lookup[file_type]["top_level_model"],
        exempt_attributes=["identifiers", "properties", "instrument_identifiers"],
    )

    # Create the thread pool to use with the async_tools.run_in_executor decorator to make sync functions awaitable
    thread_pool = ThreadPool(thread_pool_max_workers).thread_pool

    if instrument_name_enrichment:
        loop = cocoon.async_tools.start_event_loop_new_thread()

        data_frame, mapping_required = asyncio.run_coroutine_threadsafe(
            cocoon.instruments.enrich_instruments(
                api_factory=api_factory,
                data_frame=data_frame,
                instrument_identifier_mapping=identifier_mapping,
                mapping_required=mapping_required,
                constant_prefix="$",
                **{"thread_pool": thread_pool},
            ),
            loop,
        ).result()

        # Stop the additional event loop
        cocoon.async_tools.stop_event_loop_new_thread(loop)

    """
    Unnest and populate defaults where a mapping is provided with column and/or default fields in a nested dictionary
    
    e.g.
    {'name': {
        'column': 'instrument_name',
        'default': 'unknown_name'
        }
    }
    
    rather than simply
    {'name': 'instrument_name'}
    """
    (
        data_frame,
        mapping_required,
    ) = cocoon.utilities.handle_nested_default_and_column_mapping(
        data_frame=data_frame, mapping=mapping_required, constant_prefix="$"
    )
    (
        data_frame,
        mapping_optional,
    ) = cocoon.utilities.handle_nested_default_and_column_mapping(
        data_frame=data_frame, mapping=mapping_optional, constant_prefix="$"
    )

    # Get all the DataFrame columns as well as those that contain at least one null value
    data_frame_columns = list(data_frame.columns.values)
    nan_columns = [
        column for column in data_frame_columns if data_frame[column].isna().any()
    ]

    # Validate that none of the provided columns are missing or invalid
    Validator(
        mapping_required, "mapping_required"
    ).get_dict_values().filter_list_using_first_character("$").check_subset_of_list(
        data_frame_columns, "DataFrame Columns"
    ).check_no_intersection_with_list(
        nan_columns, "Columns with Missing Values"
    )

    Validator(
        mapping_optional, "mapping_optional"
    ).get_dict_values().filter_list_using_first_character("$").check_subset_of_list(
        data_frame_columns, "DataFrame Columns"
    )

    Validator(
        identifier_mapping, "identifier_mapping"
    ).get_dict_values().filter_list_using_first_character("$").check_subset_of_list(
        data_frame_columns, "DataFrame Columns"
    )

    Validator(property_columns, "property_columns").check_subset_of_list(
        data_frame_columns, "DataFrame Columns"
    )

    # Converts higher level data types such as dictionaries and lists to strings
    data_frame = data_frame.applymap(cocoon.utilities.convert_cell_value_to_string)

    if remove_white_space:
        column_list = [property_columns]
        for col in [mapping_optional, mapping_required, identifier_mapping]:
            column_list.append(col.values())

        column_list = list(set([item for sublist in column_list for item in sublist]))
        data_frame = strip_whitespace(data_frame, column_list)

    # Get the types of the attributes on the top level model for this request
    open_api_types = getattr(
        lusid.models, domain_lookup[file_type]["top_level_model"]
    ).openapi_types

    # If there is a sub_holding_keys attribute and it has a dict type this means the sub_holding_keys
    # need to have a property definition and be populated with values from the provided dataframe columns
    if (
        "sub_holding_keys" in open_api_types.keys()
        and "dict" in open_api_types["sub_holding_keys"]
    ):

        Validator(sub_holding_keys, "sub_holding_key_columns").check_subset_of_list(
            data_frame_columns, "DataFrame Columns"
        )

        # Check for and create missing property definitions for the sub-holding-keys
        data_frame = cocoon.properties.create_missing_property_definitions_from_file(
            api_factory=api_factory,
            properties_scope=properties_scope,
            domain="Transaction",
            data_frame=data_frame,
            property_columns=sub_holding_keys,
        )

    # Check for and create missing property definitions for the properties
    if domain_lookup[file_type]["domain"] is not None:
        data_frame = cocoon.properties.create_missing_property_definitions_from_file(
            api_factory=api_factory,
            properties_scope=properties_scope,
            domain=domain_lookup[file_type]["domain"],
            data_frame=data_frame,
            property_columns=property_columns,
        )

    # Start a new event loop in a new thread, this is required to run inside a Jupyter notebook
    loop = cocoon.async_tools.start_event_loop_new_thread()

    # Keyword arguments to be used in requests to the LUSID API
    keyword_arguments = {
        "scope": scope,
        # This handles that identifiers need to be specified differently based on the request type, allowing users
        # to provide either the entire key e.g. "Instrument/default/Figi" or just the code "Figi" for any request
        "full_key_format": domain_lookup[file_type]["full_key_format"],
        # Gets the allowed unique identifiers
        "unique_identifiers": cocoon.instruments.get_unique_identifiers(
            api_factory=api_factory
        ),
        "holdings_adjustment_only": holdings_adjustment_only,
        "thread_pool": thread_pool,
    }

    # Get the responses from LUSID
    responses = asyncio.run_coroutine_threadsafe(
        _construct_batches(
            api_factory=api_factory,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            property_columns=property_columns,
            properties_scope=properties_scope,
            instrument_identifier_mapping=identifier_mapping,
            batch_size=batch_size,
            file_type=file_type,
            domain_lookup=domain_lookup,
            sub_holding_keys=sub_holding_keys,
            **keyword_arguments,
        ),
        loop,
    ).result()

    # Stop the additional event loop
    cocoon.async_tools.stop_event_loop_new_thread(loop)

    return {file_type + "s": responses}
