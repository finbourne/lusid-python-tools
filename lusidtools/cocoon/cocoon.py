import asyncio
import lusid
import pandas as pd
from lusidtools import cocoon
from lusidtools.cocoon.async_tools import run_in_executor
from lusidtools.cocoon.dateorcutlabel import DateOrCutLabel
from lusidtools.cocoon.utilities import _checkargs, strip_whitespace
from lusidtools.cocoon.validator import Validator


class _BatchLoader:
    """
    This class contains all the methods used for loading data in batches. The @run_in_executor decorator makes the
    synchronous functions awaitable
    """

    @staticmethod
    @run_in_executor
    def _load_instrument_batch(
        api_factory: lusid.utilities.ApiClientFactory, instrument_batch: list, **kwargs
    ) -> lusid.models.UpsertInstrumentsResponse:
        """

        Parameters
        ----------
        api_factory:        lusid.utilities.ApiClientFactory
                            The api factory to use
        instrument_batch:   list[lusid.models.InstrumentDefinition]
                            The batch of instruments to upsert

        Returns
        -------
        UpsertInstrumentsResponse:  UpsertInstrumentsResponse
                                    The response from LUSID
        """

        # Ensure that the list of allowed unique identifiers exists
        if "unique_identifiers" not in list(kwargs.keys()):
            unique_identifiers = cocoon.instruments.get_unique_identifiers(
                api_factory=api_factory
            )
        else:
            unique_identifiers = kwargs["unique_identifiers"]

        @_checkargs
        def _get_alphabetically_first_identifier_key(
            instrument: lusid.models.InstrumentDefinition, unique_identifiers: list
        ):
            """
            Gets the alphabetically first occurring unique identifier on an instrument and use it as the correlation
            id on the request

            Parameters
            ----------
            instrument:             lusid.models.InstrumentDefinition
                                    The instrument to create a correlation id for
            unique_identifiers:     list[str]
                                    The list of allowed unique identifiers

            Returns
            -------
            correlation_id:         str
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
            instruments={
                _get_alphabetically_first_identifier_key(
                    instrument, unique_identifiers
                ): instrument
                for instrument in instrument_batch
            }
        )

    @staticmethod
    @run_in_executor
    def _load_quote_batch(
        api_factory: lusid.utilities.ApiClientFactory, quote_batch: list, **kwargs
    ) -> lusid.models.UpsertQuotesResponse:
        """
        Upserts a batch of quotes into LUSID

        Parameters
        ----------
        api_factory:    lusid.utilities.ApiClientFactory
                        The api factory to use
        quote_batch:    list[lusid.models.UpsertQuoteRequest]
                        The batch of quotes to upsert
        scope:          str
                        The scope to upsert the quotes into

        Returns
        -------
        response:       lusid.models.UpsertQuotesResponse
                        The response from LUSID

        """

        if "scope" not in list(kwargs.keys()):
            raise KeyError(
                "You are trying to load quotes without a scope, please ensure that a scope is provided."
            )

        return api_factory.build(lusid.api.QuotesApi).upsert_quotes(
            scope=kwargs["scope"],
            quotes={
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
    def _load_transaction_batch(
        api_factory: lusid.utilities.ApiClientFactory, transaction_batch: list, **kwargs
    ) -> lusid.models.UpsertPortfolioTransactionsResponse:
        """
        Upserts a batch of transactions into LUSID

        Parameters
        ----------
        api_factory:        lusid.utilities.ApiClientFactory api_factory
                            The api factory to use
        transaction_batch:  list[lusid.models.TransactionRequest]
                            The batch of transactions to upsert

        Other Parameters
        ----------------
        scope:  str
                The scope of the Transaction Portfolio to upsert the transactions into
        code:   str
                The code of the Transaction Portfolio, together with the scope this uniquely identifies the portfolio

        Returns
        -------
        Transactions_response:  lusid.models.UpsertPortfolioTransactionsResponse
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
            scope=kwargs["scope"], code=kwargs["code"], transactions=transaction_batch
        )

    @staticmethod
    @run_in_executor
    def _load_holding_batch(
        api_factory: lusid.utilities.ApiClientFactory, holding_batch: list, **kwargs
    ) -> lusid.models.HoldingsAdjustment:
        """

        Parameters
        ----------
        api_factory:        lusid.utilities.ApiClientFactory api_factory
                            The api factory to use
        holding_batch:      list[lusid.models.AdjustHoldingRequest]
                            The batch of holdings
        Other Parameters
        ----------------
        scope:          str
                        scope to upsert holdings to
        code:           str
                        code of portfolio to upload to
        effective_at:   str
                        effective at date for holdings batch

        Returns
        -------
        Holdings_adjustment:    lusid.models.HoldingsAdjustment
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

        return api_factory.build(lusid.api.TransactionPortfoliosApi).set_holdings(
            scope=kwargs["scope"],
            code=kwargs["code"],
            effective_at=str(DateOrCutLabel(kwargs["effective_at"])),
            holding_adjustments=holding_batch,
        )

    @staticmethod
    @run_in_executor
    def _load_portfolio_batch(
        api_factory: lusid.utilities.ApiClientFactory, portfolio_batch: list, **kwargs
    ) -> lusid.models.Portfolio:
        """

        Parameters
        ----------
        api_factory:        lusid.utilities.ApiClientFactory
                            The api factory to use
        portfolio_batch:    list[lusid.models.CreateTransactionPortfolioRequest]
                            The batch of portfolios to create

        Other Parameters
        ----------------
        scope:          str
                        scope to upsert holdings to
        code:           str
                        code of portfolio to upload to

        Returns
        -------
        response:       lusid.models.Portfolio
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

        try:
            return api_factory.build(lusid.api.PortfoliosApi).get_portfolio(
                scope=kwargs["scope"], code=kwargs["code"]
            )
        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                return api_factory.build(
                    lusid.api.TransactionPortfoliosApi
                ).create_portfolio(
                    scope=kwargs["scope"], transaction_portfolio=portfolio_batch[0]
                )
            # Add in here upsert portfolio properties if it does exist

    @staticmethod
    @run_in_executor
    def _load_instrument_property_batch(
        api_factory: lusid.utilities.ApiClientFactory, property_batch: list, **kwargs
    ) -> [lusid.models.UpsertInstrumentPropertiesResponse]:
        """
        Add properties to the set instruments

        identifiers will be resolved to a LusidInstrumentId, where an identifier resolves to more than one LusidInstrumentId the property will be added to all matching instruments kwargs

        Parameters
        ----------
        api_factory:        lusid.utilities.ApiClientFactory
                            The api factory to use
        property_batch:     list[lusid.models.UpsertInstrumentPropertyRequest]
                            Properties to add

        Other Parameters
        ----------------
        TODO

        Returns
        -------
        response:   [lusid.models.UpsertInstrumentPropertiesResponse]
                    The response from LUSID
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
            ).instruments_search(symbols=[search_request], mastered_only=True)

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
    api_factory:        lusid.utilities.ApiClientFactory
                        The api factory to use
    single_requests:    single_requests
                        The list of single requests for LUSID
    file_type:          str
                        The file type e.g. instruments, portfolios etc.
    Other Parameters
    ----------------
    effective_at:   str
                    date at which the request is effective for
    code:           str
                    code of portfolio to upsert to
    scope:          str
                    scope to upsert data to

    Returns
    -------
    StaticMethod:   BatchLoader
                    A static method on BatchLoader
    """

    # Dynamically call the correct async function to use based on the file type
    return await getattr(_BatchLoader, f"_load_{file_type}_batch")(
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
    data_frame:                     pd.DataFrame
                                    The DataFrame containing the data to load
    mapping_required:               dict
                                    the required mapping
    mapping_optional:               dict
                                    the optional mapping
    property_columns:               list
                                    The property columns to add as property values
    properties_scope:               str
                                    The scope to add the property values in
    instrument_identifier_mapping:  dict
                                    The mapping for the identifiers
    file_type:                      str
                                    The file type to load
    domain_lookup:                  dict
                                    The domain lookup
    Other Parameters
    ----------------
    TODO

    Returns
    -------
    single_requests:                list
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
            cocoon.utilities._populate_model(
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
    api_factory:                    lusid.utilities.ApiClientFactory
                                    The api factory to use
    data_frame:                     pd.DataFrame
                                    The DataFrame containing the data to load
    mapping_required:               dict
                                    The required mapping
    mapping_optional:               dict
                                    The optional mapping
    property_columns:               list
                                    The property columns to add as property values
    properties_scope:               str
                                    The scope to add the property values in
    instrument_identifier_mapping:  dict
                                    The mapping for the identifiers
    batch_size:                     int
                                    The batch size to use
    file_type:                      str
                                    The file type to load
    domain_lookup:                  str
                                    The file type to load
    sub_holding_keys:               list
                                    The sub holding keys to use
    Other Parameters
    ----------------
    TODO

    Returns
    -------
    responses:                      dict
                                    Contains the success responses and the errors (where API exceptions have been raised)

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


@_checkargs
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
):
    """

    Parameters
    ----------
    api_factory:                    lusid.utilities.ApiClientFactory api_factory
                                    The api factory to use
    scope:                          str
                                    The scope of the resource to load the data into
    data_frame:                     pd.DataFrame
                                    The DataFrame containing the data
    mapping_required:               dict{str, str}
                                    The dictionary mapping the DataFrame columns to LUSID's required attributes
    mapping_optional:               dict{str, str}
                                    The dictionary mapping the DataFrame columns to LUSID's optional attributes
    file_type:                      str
                                    The type of file e.g. transactions, instruments, holdings, quotes, portfolios
    identifier_mapping:             dict{str, str}
                                    The dictionary mapping of LUSID instrument identifiers to identifiers in the DataFrame
    property_columns:               list
                                    The columns to create properties for
    properties_scope:               str
                                    The scope to add the properties to
    batch_size:                     int
                                    The size of the batch to use when using upsert calls e.g. upsert instruments, upsert quotes etc.
    remove_white_space:             bool
                                    remove whitespace either side of each value in the dataframe
    instrument_name_enrichment:     bool
                                    request additional identifier information from open-figi
    sub_holding_keys:               str
                                    The sub holding keys to use for this request. Can be a list of property keys or a list of
                                    columns in the dataframe to use to create sub holdings

    Returns
    -------
    responses:                      dict
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
    TODO

    * Loading Corporate Actions
    TODO
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
    cocoon.utilities._verify_all_required_attributes_mapped(
        mapping=mapping_required,
        model_object_name=domain_lookup[file_type]["top_level_model"],
        exempt_attributes=["identifiers", "properties", "instrument_identifiers"],
    )

    if instrument_name_enrichment:
        loop = cocoon.async_tools.start_event_loop_new_thread()

        data_frame, mapping_required = asyncio.run_coroutine_threadsafe(
            cocoon.instruments.enrich_instruments(
                api_factory=api_factory,
                data_frame=data_frame,
                instrument_identifier_mapping=identifier_mapping,
                mapping_required=mapping_required,
                constant_prefix="$",
            ),
            loop,
        ).result()

        # Stop the additional event loop
        loop.stop()

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
    ) = cocoon.utilities._handle_nested_default_and_column_mapping(
        data_frame=data_frame, mapping=mapping_required, constant_prefix="$"
    )
    (
        data_frame,
        mapping_optional,
    ) = cocoon.utilities._handle_nested_default_and_column_mapping(
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
    data_frame = data_frame.applymap(cocoon.utilities._convert_cell_value_to_string)

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
    loop.stop()

    # Prefix the responses with the file type
    return {file_type + "s": responses}
