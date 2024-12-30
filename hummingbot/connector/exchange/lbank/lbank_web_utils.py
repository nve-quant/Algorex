from typing import Callable, Optional

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN, api_version: str = CONSTANTS.API_VERSION, **kwargs) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the domain to use (default is "com")
    :param api_version: the LBank API version to connect to ("v1", "v2"). The default value is "v2"
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URLS[domain][0].rstrip('/')  # Remove trailing slash if present
    return f"{base_url}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN, api_version: str = CONSTANTS.API_VERSION, **kwargs) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: the domain to use (default is "com")
    :param api_version: the LBank API version to connect to ("v1", "v2"). The default value is "v2"
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URLS[domain][0].rstrip('/')  # Remove trailing slash if present
    return f"{base_url}{path_url}"


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    """
    Gets the current server time from LBank
    :param throttler: Optional throttler to use
    :param domain: The domain to use for the request
    :return: The server time in milliseconds
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    
    try:
        response = await rest_assistant.execute_request(
            url=public_rest_url(path_url=CONSTANTS.GET_TIMESTAMP_PATH_URL),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.GET_TIMESTAMP_PATH_URL,
        )
        server_time = float(response["data"])
        return server_time
    except Exception as e:
        raise IOError(f"Error getting server time from LBank. Error: {str(e)}")


def build_api_factory(
    auth: Optional[AuthBase] = None,
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    time_provider: Optional[Callable] = None,
    domain: Optional[str] = None,
) -> WebAssistantsFactory:
    """
    Builds an API factory configured for LBank.
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    domain = domain or CONSTANTS.DEFAULT_DOMAIN
    
    if time_provider is None:
        async def time_provider_func() -> float:
            return await get_current_server_time(throttler=throttler, domain=domain)
        time_provider = time_provider_func
    
    api_factory = WebAssistantsFactory(
        auth=auth,
        throttler=throttler,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider)
        ],
    )
    return api_factory
