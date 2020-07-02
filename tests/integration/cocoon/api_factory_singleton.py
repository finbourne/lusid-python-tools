import threading
from pathlib import Path
import lusid


class ApiFactorySingleton:

    __lock = threading.Lock()
    __api_factory = None

    @classmethod
    def get_api_factory(cls):
        if cls.__api_factory is None:
            with cls.__lock:
                if cls.__api_factory is None:
                    secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
                    cls.__api_factory = lusid.utilities.ApiClientFactory(
                        api_secrets_filename=secrets_file
                    )
                    print(f"****************************************************")
                    print(f"                   ApiFactory {threading.current_thread().name} [{threading.get_ident()}]")
                    print(f"****************************************************")
        return cls.__api_factory
