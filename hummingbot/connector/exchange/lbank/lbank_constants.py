from decimal import Decimal
from typing import Dict

from Crypto.PublicKey import RSA
from pydantic import Field, root_validator, validator
from pydantic.types import SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# Add DEFAULT_DOMAIN since LBank has multiple domains
DEFAULT_DOMAIN = "com"

# Update REST URLs to use domains like Binance
REST_URLS = {
    "com": [
        "https://api.lbkex.com/",
        "https://api.lbank.info/", 
        "https://www.lbkex.net/"
    ]
}

# Add server time URL since LBank requires timestamp sync
SERVER_TIME_PATH_URL = "/v2/timestamp.do"

API_VERSION = "v2"

HBOT_ORDER_ID_PREFIX = "LBK-"

LBANK_ORDER_BOOK_SNAPSHOT_DEPTH = 60

# URLs
REST_URL = "https://api.lbkex.com"
WSS_URL = "wss://www.lbkex.net/ws/V2/"

# Public REST Endpoints
GET_TIMESTAMP_PATH_URL = "/v2/timestamp.do"
ORDER_BOOK_PATH_URL = "/v2/supplement/incrDepth.do"
TRADING_PAIRS_PATH_URL = "/v2/currencyPairs.do"
CURRENT_MARKET_DATA_PATH_URL = "/v2/ticker/24hr.do"
TICKER_PRICE_CHANGE_PATH_URL = "/v2/ticker/24hr"
EXCHANGE_INFO_PATH_URL = "/v2/accuracy.do"
TICKER_BOOK_PATH_URL = "/v2/ticker.do"
TICKER_PRICE_PATH_URL = "/v2/supplement/ticker/price.do"

# Private REST Endpoints
ACCOUNTS_PATH_URL = "/v2/supplement/user_info.do"
CREATE_ORDER_PATH_URL = "/v2/supplement/create_order.do"
CANCEL_ORDER_PATH_URL = "/v2/supplement/cancel_order.do"
CREATE_LISTENING_KEY_PATH_URL = "/v2/subscribe/get_key.do"
REFRESH_LISTENING_KEY_PATH_URL = "/v2/subscribe/refresh_key.do"
ORDER_UPDATES_PATH_URL = "/v2/supplement/orders_info.do"
CANCEL_ORDERS_BY_PAIR_PATH_URL = "/v2/supplement/cancel_order_by_symbol.do"
TRADE_UPDATES_PATH_URL = "/v2/supplement/transaction_history.do"
ORDER_INFO_HISTORY_PATH_URL = "/v2/supplement/orders_info_history.do"

# WebSocket channels
ORDER_BOOK_DEPTH_CHANNEL = "depth"
ORDER_BOOK_TRADE_CHANNEL = "trade"
USER_ORDER_UPDATE_CHANNEL = "orderUpdate"
USER_BALANCE_UPDATE_CHANNEL = "assetUpdate"
PING_RESPONSE = "ping"

# WebSocket configurations
WS_PING_REQUEST_INTERVAL = 15  # seconds
LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800000  # 30 minutes in milliseconds
ORDER_BOOK_DEPTH_CHANNEL_DEPTH = 50
ORDER_BOOK_SNAPSHOT_DEPTH = 50

# Authentication
AUTH_METHODS = {"RSA", "HmacSHA256"}

# Misc Information
MAX_ORDER_ID_LEN = 50
CLIENT_ID_PREFIX = "HBOT-"

# Update rate limit constants
TEN_SECONDS = 10
ORDER_LIMITS = 500  # per 10 seconds
GLOBAL_LIMITS = 200  # per 10 seconds

# Rate Limits
RATE_LIMITS = [
    # Order endpoints - 500 requests per 10 seconds
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=ORDER_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=ORDER_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=ORDER_INFO_HISTORY_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    
    # All other endpoints - 200 requests per 10 seconds
    RateLimit(limit_id=ORDER_BOOK_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=TRADING_PAIRS_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=CREATE_LISTENING_KEY_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=REFRESH_LISTENING_KEY_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=ORDER_UPDATES_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=TRADE_UPDATES_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=TICKER_BOOK_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=TICKER_PRICE_PATH_URL, limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    
    # Server time endpoint - 100 requests per second
    RateLimit(limit_id=GET_TIMESTAMP_PATH_URL, limit=100, time_interval=1, weight=1),
    
    # User endpoints
    RateLimit(limit_id="/v2/supplement/user_info.do", limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id="/v2/supplement/transaction_history.do", limit=GLOBAL_LIMITS, time_interval=TEN_SECONDS, weight=1),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=10, time_interval=1),
]


# Error Codes
ERROR_CODES = {
    10000: "Internal error",
    10001: "The required parameters can not be empty",
    10002: "Validation failed",
    10003: "Invalid parameter",
    10004: "Request too frequent",
    10005: "Secret key does not exist",
    10006: "User does not exist",
    10007: "Invalid signature",
    10008: "Invalid Trading Pair",
    10009: "Price and/or Amount are required for limit order",
    10010: "Price and/or Amount must be more than 0",
    # Add more error codes as needed
}

ORDER_NOT_FOUND_ERROR_CODE = 10025
ORDER_NOT_EXIST_ERROR_MESSAGE = "order does not exist"

# According to LBank's fee documentation
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),  # 0.1% maker fee
    taker_percent_fee_decimal=Decimal("0.001"),  # 0.1% taker fee
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

# Add ORDER_STATE mapping
ORDER_STATE = {
    "0": OrderState.OPEN,  # Initial state
    "1": OrderState.PARTIALLY_FILLED,  # Partially filled
    "2": OrderState.FILLED,  # Completely filled
    "3": OrderState.CANCELED,  # Cancelled
    "4": OrderState.FAILED,  # Order failed
    "-1": OrderState.FAILED  # Error state
}

class LbankConfigMap(BaseConnectorConfigMap):
    connector: str = Field(
        default="lbank",
        const=True,
        client_data=None
    )
    
    lbank_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your LBank API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    
    lbank_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your LBank secret key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    
    lbank_auth_method: str = Field(
        default="HmacSHA256",
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter the authentication method (HmacSHA256 or RSA)",
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "lbank"

    @validator("lbank_auth_method", pre=True)
    def validate_auth_method(cls, value: str):
        """Used for client-friendly error output."""
        if value not in ["RSA", "HmacSHA256"]:
            raise ValueError("Authentication Method not supported. Supported methods are RSA/HmacSHA256")
        return value

    @root_validator()
    def post_validations(cls, values: Dict):
        auth_method = values.get("lbank_auth_method")
        if auth_method == "RSA":
            secret_key = values.get("lbank_secret_key")
            if secret_key is not None:
                try:
                    RSA.importKey(secret_key.get_secret_value())
                except Exception as e:
                    raise ValueError(f"Unable to import RSA keys. Error: {str(e)}")
        return values

KEYS = LbankConfigMap.construct()

ORDER_TYPE_MAP = {
    "limit": "limit",
    "market": "market"
}
