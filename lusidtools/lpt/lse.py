import datetime
import importlib
import inspect
import json
import os

from . import lpt
from .either import Either
from .record import Rec


def ApiConverter(connection, swagger_api):
    def check(f):
        return (inspect.isfunction(f) and f.__name__.endswith("_with_http_info")) or (
            inspect.ismethod(f) and f.__name__.endswith("_with_http_info")
        )

    d = {}

    for api in [i[1] for i in swagger_api.__dict__.items() if i[0].endswith("Api")]:
        jlh = api(connection)
        for n, v in inspect.getmembers(jlh, predicate=check):
            d[n[:-15]] = v
    f = Rec(**d)

    return f


class ExtendedAPI:
    # Constructor
    def __init__(self, config, api, lusid, custom_headers=None):
        class dummy(Exception):
            pass

        self.api = ApiConverter(api, lusid)
        self.models = lusid.models
        self.lusid = lusid
        self.stats_file = config.get("stats", "")
        self.stats = [] if self.stats_file != "" else None

        # See if the api contains the ErrorResponseException

        exc = (
            lusid.rest.ApiException if "ApiException" in lusid.rest.__dict__ else dummy
        )

        self.call = Caller(self.api, self.stats, exc)
        self.call.custom_headers = custom_headers
        self.call.as_at = config.get("asat")

    # Create array of objects using the models
    def from_df(self, df, model, related=None):
        return lpt.from_df(df, model, self.models.__dict__, related)

    def dump_stats(self):
        if self.stats != None:
            lpt.dump_stats(
                self.stats_file,
                self.stats,
                [
                    "startTime",
                    "endTime",
                    "name",
                    "requestId",
                    "duration",
                    "elapsed",
                    "status",
                ],
            )


# Wrapper class to call an API function returning the stats
class Caller:
    def __init__(self, api, stats, exceptionClass):
        self.api = api
        self.stats = stats
        self.exceptionClass = exceptionClass
        self.custom_headers = None
        self.as_at = None

    def __getattr__(self, name):
        fn = getattr(self.api, name)

        # Function that will call the target function
        # returns an Either
        def callApiFn(*args, **kwargs):
            # Add the ' parameter and custom headers
            # adjKwargs = dict(raw=True,custom_headers=self.custom_headers)
            # adjKwargs.update(kwargs)
            adjKwargs = dict([v for v in dict(kwargs).items() if v[1] != None])

            # Add the as_at parameter if provided and valid
            if self.as_at:  # and signature(fn).parameters.get('as_at'):
                adjKwargs.update({"as_at": lpt.to_date(self.as_at)})

            # Measure execution time of the call
            startTime = datetime.datetime.now()
            try:
                result = fn(*args, **(adjKwargs))
                request_id = result[2].get("lusid-meta-requestId", "n/a")
            except self.exceptionClass as err:
                data = {} if err.body == "" or err.body == b"" else json.loads(err.body)

                instance = data.get("instance", "n/a")
                s = instance.split("insights/logs/")
                request_id = "n/a" if len(s) != 2 else s[1]

                result = [
                    Rec(
                        status=err.status,
                        reason=err.reason,
                        code=data.get("code", "n/a"),
                        message=data.get("title", "n/a"),
                        detailed_message=data.get("detail", "n/a"),
                        items=data.get("errorDetails", []),
                        instance=instance,
                        requestId=request_id,
                    ),
                    err.status,
                    {},
                ]

            endTime = datetime.datetime.now()

            statistics = Rec(
                name=name,
                startTime=startTime,
                endTime=endTime,
                duration=(endTime - startTime).total_seconds(),
                elapsed=float(result[2].get("lusid-meta-duration", 0)) / 1000,
                status=result[1],
                requestId=request_id,
            )

            if self.stats != None:
                self.stats.append(statistics)

            # If successful, return the output as a 'right'
            if result[2].get("lusid-meta-success", "False") == "True":
                return Either.Right(Rec(stats=statistics, content=result[0],))
            # Otherwise return a 'left' (a failure)
            return Either.Left(result[0])

        return callApiFn


# Connect to appropriate LUSID environment
def connect(*args, **kwargs):
    # start with no overrides
    overrides = {}

    # include dictionary types
    for i in args:
        overrides.update(i.__dict__)

    # and keyword overrides
    overrides.update(kwargs)

    # Get basic settings from the secrets file
    try:
        with open(overrides.get("secrets", "secrets.json"), "r") as secrets:
            settings = json.load(secrets)
            # apply the overrides
            settings.update(overrides)
    except:
        settings = overrides  # connection may not require secrets. Don't fail (yet)

    # import the appropriate environment for the connection
    env = settings.get("env", ["lusid"])
    connect = importlib.import_module(".connect_{}".format(env[0]), "lusidtools.lpt")

    api = ExtendedAPI(settings, *connect.connect(settings))

    # For debugging purposes, this will display
    if os.getenv("LSE_DEBUG_BODY", "N") == "Y":

        def serializer(standard):
            def fn(request, model):
                s = standard(request, model)
                print(s)
                return s

            return fn

        api.api._serialize.body = serializer(api.api._serialize.body)

    return api
