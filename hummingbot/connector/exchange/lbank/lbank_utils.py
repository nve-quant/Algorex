from decimal import Decimal
from typing import Any, Dict

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

from .lbank_constants import LbankConfigMap

# These are the exact attributes create_connector_settings() looks for:
CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"
DEFAULT_FEES = [0.1, 0.1]  # [maker_percent, taker_percent]
USE_ETHEREUM_WALLET = False
USE_ETH_GAS_LOOKUP = False

# Use the config map from constants
KEYS = LbankConfigMap.construct()

# Optional: If you have multiple domains
OTHER_DOMAINS = []
OTHER_DOMAINS_PARAMETER = {}
OTHER_DOMAINS_EXAMPLE_PAIR = {}
OTHER_DOMAINS_DEFAULT_FEES = {}
OTHER_DOMAINS_KEYS = {}

# Helper functions remain the same
def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    return all(
        key in exchange_info
        for key in [
            "symbol",
            "priceAccuracy",
            "quantityAccuracy"
        ]
    )

def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.lower().replace("-", "_")

def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    return exchange_trading_pair.upper().replace("_", "-")
