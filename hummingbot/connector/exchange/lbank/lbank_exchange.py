import asyncio
import hashlib
import hmac
import json
import math
import random
import string
import sys
import time
import urllib.parse
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from async_timeout import timeout
from bidict import bidict
from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS, lbank_utils, lbank_web_utils as web_utils
from hummingbot.connector.exchange.lbank.lbank_api_order_book_data_source import LbankAPIOrderBookDataSource
from hummingbot.connector.exchange.lbank.lbank_api_user_stream_data_source import LbankAPIUserStreamDataSource
from hummingbot.connector.exchange.lbank.lbank_auth import LbankAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, RESTResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class LbankExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    @classmethod
    def name(cls) -> str:
        return "lbank"

    @classmethod
    def is_centralized(cls) -> bool:
        return True

    @classmethod
    def conf_path(cls) -> str:
        return "conf/connectors/lbank.yml"

    @classmethod
    def get_connector_config_map(cls) -> Dict:
        return {
            "lbank_api_key": ConfigVar(
                key="lbank_api_key",
                prompt="Enter your LBank API key >>> ",
                is_secure=True,
                is_connect_key=True,
                prompt_on_new=True,
            ),
            "lbank_secret_key": ConfigVar(
                key="lbank_secret_key", 
                prompt="Enter your LBank secret key >>> ",
                is_secure=True,
                is_connect_key=True,
                prompt_on_new=True,
            ),
            "lbank_auth_method": ConfigVar(
                key="lbank_auth_method",
                prompt="Enter your LBank auth method (RSA/HmacSHA256) >>> ",
                is_secure=False,
                is_connect_key=True,
                prompt_on_new=True,
                default="HmacSHA256"
            ),
        }

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 lbank_api_key: str,
                 lbank_secret_key: str,
                 lbank_auth_method: str = "HmacSHA256",
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        """Initialize the LBank exchange."""
        self._api_key = lbank_api_key
        self._secret_key = lbank_secret_key
        self._auth_method = lbank_auth_method
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        
        # Initialize the trading pair maps before calling super().__init__
        self._trading_pair_symbol_map = None
        self.symbol_trading_pair_map = {}
        
        super().__init__(client_config_map)
        
        # Initialize timestamps
        self._last_trades_poll_lbank_timestamp = 0
        self._last_poll_timestamp = 0
        
        # Initialize URL endpoints using correct constant names
        self._user_info_url = CONSTANTS.ACCOUNTS_PATH_URL
        self._trade_history_url = CONSTANTS.TRADE_UPDATES_PATH_URL
        self._user_trades_url = CONSTANTS.TRADE_UPDATES_PATH_URL
        
        # Initialize auth
        self._auth = LbankAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            auth_method=self._auth_method,
        )
        
        # Initialize web assistants factory
        self._web_assistants_factory = web_utils.build_api_factory(
            auth=self._auth,
            domain=self._domain,
        )
        
        # Initialize trackers
        self._order_book_tracker = None
        self._user_stream_tracker = None
        self._account_balances = {}
        self._account_available_balances = {}
        
        # Initialize user stream tracker if trading is required
        if self._trading_required:
            self._user_stream_tracker = self._create_user_stream_tracker()

    def _create_user_stream_tracker(self) -> UserStreamTrackerDataSource:
        """
        Creates a new user stream tracker instance.
        """
        user_stream_tracker = LbankAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )
        return user_stream_tracker

    @staticmethod
    def lbank_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(lbank_type: str) -> OrderType:
        return OrderType[lbank_type]

    def _time_provider(self) -> int:
        """
        Provide current time (in ms) without referencing any unpickleable coroutine or lambda.
        """
        return int(self._time_synchronizer.time() * 1e3)

    @property
    def authenticator(self) -> LbankAuth:
        """
        Authenticator property to handle authentication for API requests
        """
        return LbankAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            auth_method=self._auth_method,
            time_provider=self._time_provider
        )

    @property
    def name(self) -> str:
        return "lbank"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.GET_TIMESTAMP_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def account_balances(self) -> Dict[str, Decimal]:
        """
        Returns a dictionary of token balances with default decimal values
        """
        return {k: v if v is not None else s_decimal_0 
                for k, v in self._account_balances.items()}

    @property
    def account_available_balances(self) -> Dict[str, Decimal]:
        """
        Returns a dictionary of available token balances with default decimal values
        """
        return {k: v if v is not None else s_decimal_0 
                for k, v in self._account_available_balances.items()}

    def get_balance(self, currency: str) -> Decimal:
        """
        Gets balance for a specific currency, returns 0 if not found
        """
        balance = self._account_balances.get(currency, s_decimal_0)
        return balance if balance is not None else s_decimal_0

    def get_available_balance(self, currency: str) -> Decimal:
        """
        Gets available balance for a specific currency, returns 0 if not found
        """
        balance = self._account_available_balances.get(currency, s_decimal_0)
        return balance if balance is not None else s_decimal_0

    def get_all_balances(self) -> Dict[str, Decimal]:
        """
        Return a dictionary of all account balances
        Used by balance_command.py to get balances and calculate USD values using RateOracle
        """
        return self._account_balances.copy()

    def get_balance_display(self) -> str:
        """
        Helper method to format the balance display string
        """
        lines = []
        total_value = Decimal("0")
        
        # Header
        lines.append("lbank:")
        lines.append(f"    {'Asset':<6} {'Amount':>12} {'USD Value':>12} {'Allocated'}")
        
        # Asset rows
        for asset, balance in sorted(self._account_balances.items()):
            usd_value = self._account_balances_usd_value.get(asset, Decimal("0"))
            total_value += usd_value
            
            # Format with exact spacing to match client output
            balance_str = f"{balance:>12.4f}"
            usd_value_str = f"{usd_value:>12.2f}"
            
            lines.append(f"    {asset:<6} {balance_str} {usd_value_str} {'100%'}")
        
        # Add total value
        lines.append("")
        lines.append(f"Total: $ {total_value:.2f}")
        lines.append("Allocated: 0.00%")
        
        return "\n".join(lines)

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        """
        Gets ticker prices for all trading pairs from LBank
        :return: List of dictionaries containing ticker information
        """
        # LBank requires a symbol parameter even when getting all tickers
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_BOOK_PATH_URL,
            params={"symbol": "all"}  # "all" returns data for all trading pairs
        )
        
        if not isinstance(response, list):
            raise Exception(f"Unexpected response format from ticker endpoint: {response}")
        
        return response

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """
        Creates a user stream data source instance for user stream tracking.
        """
        return LbankAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self.trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    def get_lbank_order_type(self, trade_type: TradeType, order_type: OrderType) -> str:
        """
        Converts Hummingbot order types to LBank order types
        :param trade_type: The trade type (BUY/SELL)
        :param order_type: The order type (LIMIT/MARKET/LIMIT_MAKER)
        :return: LBank order type string
        """
        if order_type == OrderType.LIMIT_MAKER:
            # LBank uses buy_maker/sell_maker for post-only orders
            return "buy_maker" if trade_type == TradeType.BUY else "sell_maker"
        elif order_type == OrderType.LIMIT:
            return "buy" if trade_type == TradeType.BUY else "sell"
        elif order_type == OrderType.MARKET:
            return "buy_market" if trade_type == TradeType.BUY else "sell_market"
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

    def _sort_params_by_name(self, params: dict) -> dict:
        """
        Sort parameters by comparing each letter of parameter names.
        If first letters are same, compare second letters, and so on.
        """
        return dict(sorted(params.items()))

    def _generate_order_id(self, side: str, trading_pair: str) -> str:
        """Generate a unique order ID based on timestamp and random string."""
        ts = int(time.time() * 1000)
        rand_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        return f"LB-{side}-{trading_pair}-{ts}-{rand_id}"

    async def _create_order(
            self,
            trade_type: TradeType,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType,
            price: Decimal = s_decimal_0,
            **kwargs) -> Dict[str, Any]:
        """
        Creates an order in the exchange using the parameters to configure it
        """
        try:
            exchange_trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair)
            
            self.logger().info(f"Creating order - Pair: {exchange_trading_pair}, Type: {trade_type}, Amount: {amount}, Price: {price}")
            
            # Determine order type
            type_str = "buy_maker" if trade_type == TradeType.BUY else "sell_maker"
            
            # Create base parameters
            params = {
                "symbol": exchange_trading_pair,
                "type": type_str,
                "price": str(price),
                "amount": str(amount),
                "custom_id": order_id
            }
            
            # Create REST request - let auth flow handle the signature
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            request = RESTRequest(
                method=RESTMethod.POST,
                url=web_utils.private_rest_url(path_url=CONSTANTS.CREATE_ORDER_PATH_URL),
                data=params,  # Pass raw params, let auth handle it
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                is_auth_required=True
            )

            self.logger().info(f"Sending order request: {request}")
            raw_response = await rest_assistant.call(request)
            
            response = await raw_response.json()
            self.logger().info(f"Order response: {response}")
            
            if not response.get("result", False):
                error_msg = response.get("msg", "Unknown error")
                error_code = response.get("error_code", "Unknown code")
                self.logger().error(f"Order creation failed: {error_msg} (code: {error_code})")
                raise ValueError(f"LBank API error: {error_msg} (code: {error_code})")
            
            return response

        except Exception as e:
            self.logger().error(f"Error creating order {order_id}: {str(e)}", exc_info=True)
            raise

    async def _get_current_server_time(self) -> float:
        """
        Gets the current server time from LBank
        Returns a coroutine that will resolve to the server time
        """
        response = await self._api_get(
            path_url=CONSTANTS.GET_TIMESTAMP_PATH_URL,
            limit_id=CONSTANTS.GET_TIMESTAMP_PATH_URL
        )
        return float(response.get("data", 0))

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            **kwargs) -> Tuple[str, float]:
        """
        Place an order with logging and error handling
        """
        try:
            self.logger().info(
                f"Placing {trade_type.name} {order_type.name} order {order_id} "
                f"for {trading_pair} at {price} for {amount}"
            )
            
            creation_response = await self._create_order(
                trade_type=trade_type,
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                **kwargs
            )
            
            self.logger().info(f"Full creation response: {creation_response}")
            
            # Extract order_id from the correct response field
            exchange_order_id = str(creation_response.get("data", {}).get("order_id", ""))
            if not exchange_order_id:
                self.logger().error(f"No order_id in response: {creation_response}")
                raise ValueError("No order_id received from exchange")
                
            timestamp = self._time_synchronizer.time()
            
            self.logger().info(f"Order {order_id} placed successfully with exchange ID: {exchange_order_id}")
            return exchange_order_id, timestamp
            
        except Exception as e:
            self.logger().error(f"Error placing order {order_id}: {str(e)}", exc_info=True)
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """
        Cancels an order on LBank exchange.
        """
        try:
            self.logger().info(f"Canceling order {order_id} for {tracked_order.trading_pair}")
            
            exchange_trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
            
            # Create cancel order parameters
            params = {
                "symbol": exchange_trading_pair,
                "order_id": tracked_order.exchange_order_id
            }
            
            # Create REST request
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            request = RESTRequest(
                method=RESTMethod.POST,
                url=web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL),
                data=params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                is_auth_required=True
            )

            self.logger().info(f"Sending cancel request for order {order_id}: {request}")
            raw_response = await rest_assistant.call(request)
            
            response = await raw_response.json()
            self.logger().info(f"Cancel order response for {order_id}: {response}")
            
            # Check if the cancellation was successful
            if response.get("result") is True:
                self.logger().info(f"Successfully canceled order {order_id}")
                return True
            else:
                error_msg = response.get("msg", "Unknown error")
                error_code = response.get("error_code", "Unknown code")
                self.logger().error(f"Failed to cancel order {order_id}: {error_msg} (code: {error_code})")
                return False

        except Exception as e:
            self.logger().error(f"Error canceling order {order_id}: {str(e)}", exc_info=True)
            return False

    async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.
        
        :param exchange_info: The json API response
        :return A dictionary of trading pairs mapping to their respective TradingRule
        """
        try:
            trading_rules = {}
            
            # LBank returns a list of trading pairs in their info
            for pair_info in exchange_info.get("data", []):
                try:
                    if not lbank_utils.is_exchange_information_valid(pair_info):
                        continue
                    
                    trading_pair = lbank_utils.convert_from_exchange_trading_pair(pair_info["symbol"])
                    
                    # Extract min order size and price precision
                    min_quantity = Decimal(str(pair_info.get("minTranQua", "0")))
                    price_precision = Decimal(str(pair_info.get("priceAccuracy", "0")))
                    quantity_precision = Decimal(str(pair_info.get("quantityAccuracy", "0")))
                    
                    # Create TradingRule instance
                    trading_rules[trading_pair] = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_quantity,
                        min_price_increment=Decimal("1") / Decimal(str(10 ** price_precision)),
                        min_base_amount_increment=Decimal("1") / Decimal(str(10 ** quantity_precision)),
                        min_notional_size=Decimal("0"),  # LBank doesn't specify this
                        min_order_value=Decimal(str(pair_info.get("minTranAmt", "0"))),
                        max_order_size=Decimal(str(pair_info.get("maxTranQua", "inf"))),
                    )
                    
                except Exception as e:
                    self.logger().error(f"Error parsing trading pair rule {pair_info}: {str(e)}")
                    continue
                
            return trading_rules
            
        except Exception as e:
            self.logger().error(f"Error formatting trading rules: {str(e)}", exc_info=True)
            raise

    async def _update_trading_rules(self):
        """
        Updates the trading rules by fetching the latest exchange information
        """
        try:
            exchange_info = await self._api_get(
                path_url=CONSTANTS.TRADING_PAIRS_PATH_URL,
                limit_id=CONSTANTS.TRADING_PAIRS_PATH_URL
            )
            
            # Validate the response
            if exchange_info is None:
                self.logger().warning("No exchange info received from LBank")
                return
            
            # Log the raw response for debugging
            self.logger().debug(f"Trading rules response: {exchange_info}")
            
            # Format the trading rules
            trading_rules = await self._format_trading_rules(exchange_info)
            
            # Validate trading rules
            if not trading_rules:
                self.logger().warning("No trading rules obtained from exchange info")
                return
            
            # Update the trading rules
            self._trading_rules.clear()
            for trading_pair, trading_rule in trading_rules.items():
                self._trading_rules[trading_pair] = trading_rule
            
            # Log success
            self.logger().info("Trading rules updated successfully")
            
        except Exception as e:
            self.logger().error(f"Error updating trading rules: {str(e)}", exc_info=True)
            raise

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        Process user stream events from WebSocket
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")
                
                if event_type == "orderUpdate":
                    order_data = event_message.get("orderUpdate", {})
                    client_order_id = order_data.get("customerID")
                    exchange_order_id = order_data.get("uuid")
                    
                    self.logger().info(f"Processing order update - Client ID: {client_order_id}, Exchange ID: {exchange_order_id}")
                    
                    # Map LBank order status to our order state
                    order_status = int(order_data.get("orderStatus", 0))
                    new_state = CONSTANTS.ORDER_STATE.get(str(order_status))
                    
                    if not new_state:
                        self.logger().error(f"Unknown order status from LBank: {order_status}")
                        continue
                        
                    order_update = OrderUpdate(
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=order_data["symbol"].replace("_", "-").upper(),
                        update_timestamp=float(order_data["updateTime"]) * 1e-3,
                        new_state=new_state,
                    )
                    
                    self.logger().info(f"Created order update: {order_update}")
                    
                    # Process the order update
                    self._order_tracker.process_order_update(order_update)
                    
                    # Log the current state of order tracker
                    self.logger().info(f"Active orders after update: {list(self._order_tracker.active_orders.keys())}")
                    
                elif event_type == "assetUpdate":
                    # Process balance updates
                    balance_data = event_message.get("data", {})
                    asset = balance_data.get("assetCode", "").upper()
                    free_balance = Decimal(str(balance_data.get("free", "0")))
                    total_balance = Decimal(str(balance_data.get("asset", "0")))
                    
                    self._account_available_balances[asset] = free_balance
                    self._account_balances[asset] = total_balance
                    
                    self.logger().info(f"Updated balance for {asset}: Available = {free_balance}, Total = {total_balance}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener: {str(e)}", exc_info=True)
                await asyncio.sleep(5.0)

    async def _update_order_fills_from_trades(self):
        """
        Update order fills from order history.
        """
        try:
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
                
                # Create base parameters
                params = {
                    "symbol": exchange_symbol,
                    "current_page": "1",
                    "page_length": "100"
                }
                
                # Create REST request - let auth flow handle the signature
                rest_assistant = await self._web_assistants_factory.get_rest_assistant()
                request = RESTRequest(
                    method=RESTMethod.POST,
                    url=web_utils.private_rest_url(path_url=CONSTANTS.ORDER_INFO_HISTORY_PATH_URL),
                    data=params,  # Pass raw params, let auth handle it
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    is_auth_required=True
                )

                self.logger().info(f"Requesting order fills for {trading_pair}")
                raw_response = await rest_assistant.call(request)
                
                response = await raw_response.json()
                self.logger().debug(f"Order fills response for {trading_pair}: {response}")

                if response.get("result") is True and "data" in response:
                    orders_data = response["data"].get("orders", [])  # Get orders from the correct field
                    if not isinstance(orders_data, list):
                        self.logger().error(f"Unexpected orders array format: {orders_data}")
                        continue

                    for order in orders_data:
                        try:
                            # Validate order data
                            if not isinstance(order, dict):
                                self.logger().error(f"Invalid order data format: {order}")
                                continue

                            status = str(order.get("status", "0"))
                            if status not in ["1", "2", "3"]:  # Only process orders with valid status
                                continue

                            executed_amount = Decimal(str(order.get("deal_amount", "0")))
                            if executed_amount == Decimal("0"):
                                continue

                            # Calculate price and amounts
                            price = Decimal(str(order.get("price", "0")))
                            quote_amount = executed_amount * price

                            # Determine trade type
                            trade_type = TradeType.BUY if order.get("type", "").lower() == "buy" else TradeType.SELL

                            # Create trade update
                            trade_update = TradeUpdate(
                                trade_id=f"{order.get('order_id', '')}_{order.get('create_time', '')}",
                                client_order_id=order.get("custom_id", ""),
                                exchange_order_id=str(order.get("order_id", "")),
                                trading_pair=trading_pair,
                                fee=TradeFeeBase.new_spot_fee(
                                    fee_schema=self.trade_fee_schema(),
                                    trade_type=trade_type,
                                ),
                                fill_base_amount=executed_amount,
                                fill_quote_amount=quote_amount,
                                fill_price=price,
                                fill_timestamp=float(order.get("create_time", self._time())) * 1e-3,
                            )
                            
                            self._order_tracker.process_trade_update(trade_update)
                            self.logger().info(f"Processed trade update for order {order.get('order_id', '')}")
                            
                        except Exception as e:
                            self.logger().error(f"Error processing order {order}: {str(e)}")
                            continue

                else:
                    error_msg = response.get("msg", "Unknown error")
                    error_code = response.get("error_code", "Unknown code")
                    self.logger().error(f"Error in order history response: {error_msg} (code: {error_code})")
            
            self._last_trades_poll_lbank_timestamp = self._time()
                
        except Exception as e:
            self.logger().error(f"Error fetching order history update: {str(e)}", exc_info=True)
            raise

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = int(order.exchange_order_id)
            trading_pair = lbank_utils.convert_to_exchange_trading_pair(order.trading_pair)
            
            # Prepare parameters according to LBank API requirements
            data = {
                "orderId": str(exchange_order_id),
                "symbol": trading_pair,
            }
            
            # Add auth parameters and get headers
            auth_headers = self._auth.header_for_authentication()
            data = self._auth.add_auth_to_params(data)
            
            # Ensure Content-Type header is set
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                **auth_headers
            }
            
            # URL encode the data
            encoded_data = urlencode(data)
            
            all_fills_response = await self._api_post(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                data=encoded_data,  # Send URL-encoded data
                headers=headers,
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PATH_URL)

            if isinstance(all_fills_response, dict) and all_fills_response.get("result") == "true":
                trades = all_fills_response.get("data", [])
                for trade in trades:
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=trade.get("fee_coin", ""),
                        flat_fees=[TokenAmount(
                            amount=Decimal(str(trade.get("fee", "0"))),
                            token=trade.get("fee_coin", "")
                        )]
                    )
                    trade_update = TradeUpdate(
                        trade_id=str(trade.get("trade_id", "")),
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(trade.get("order_id", "")),
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(str(trade.get("amount", "0"))),
                        fill_quote_amount=Decimal(str(trade.get("total", "0"))),
                        fill_price=Decimal(str(trade.get("price", "0"))),
                        fill_timestamp=float(trade.get("time", 0)) * 1e-3,
                    )
                    trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Requests order status using the supplement endpoint with proper authentication
        """
        # Test logger at start of method
        self.logger().error("LOGGER TEST - START OF ORDER STATUS")
        print("PRINT TEST - START OF ORDER STATUS", flush=True)
        
        # Write to a file directly as a last resort
        with open("debug_log.txt", "a") as f:
            f.write("\n=== Starting Order Status Request ===\n")
        
        try:
            data = {
                "api_key": self._api_key,
                "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair),
            }

            if tracked_order.exchange_order_id:
                data["orderId"] = tracked_order.exchange_order_id
            else:
                data["origClientOrderId"] = tracked_order.client_order_id

            # Write to debug file
            with open("debug_log.txt", "a") as f:
                f.write(f"Request Data: {data}\n")

            self.logger().error(f"[ORDER STATUS] Requesting with data: {data}")
            print(f"Request Data: {data}", flush=True)

            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            print("Got REST assistant, executing request...", flush=True)
            
            raw_response = await rest_assistant.execute_request_and_get_response(
                url=web_utils.private_rest_url(path_url=CONSTANTS.ORDER_INFO_URL),
                data=data,
                method=RESTMethod.POST,
                is_auth_required=True,  # This will add the sign parameter
                throttler_limit_id=CONSTANTS.ORDER_INFO_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )

            print(f"Got raw response with status: {raw_response.status}", flush=True)
            self.logger().error(f"[ORDER STATUS] Response status: {raw_response.status}")
            self.logger().error(f"[ORDER STATUS] Response headers: {raw_response.headers}")
            sys.stdout.flush()
            sys.stderr.flush()

            raw_text = await raw_response.text()
            print(f"Raw response text: {raw_text}", flush=True)
            self.logger().error(f"[ORDER STATUS] Raw response text: {raw_text}")
            sys.stdout.flush()
            sys.stderr.flush()

            try:
                response = json.loads(raw_text)
                print(f"Parsed JSON response: {response}", flush=True)
                self.logger().error(f"[ORDER STATUS] Parsed JSON response: {response}")
            except json.JSONDecodeError as e:
                print(f"JSON Parse Error: {str(e)} - Raw text: {raw_text}", flush=True)
                self.logger().error(f"[ORDER STATUS] JSON Parse Error: {str(e)} - Raw text: {raw_text}")
                raise ValueError(f"Invalid JSON response: {raw_text}")

            if isinstance(response, dict):
                if not response.get("result", False):
                    error_msg = response.get("msg", "Unknown error")
                    error_code = response.get("error_code", "Unknown code")
                    error_details = (
                        f"Error code: {error_code}\n"
                        f"Error message: {error_msg}\n"
                        f"Request data: {data}\n"
                        f"Full response: {response}"
                    )
                    print(f"Request Failed: {error_details}", flush=True)
                    self.logger().error(f"[ORDER STATUS] Request failed:\n{error_details}")
                    sys.stdout.flush()
                    sys.stderr.flush()
                    raise Exception(f"Error getting order status: {error_msg} (code: {error_code})")

            order_data = response.get("data", {})
            new_state = CONSTANTS.ORDER_STATE[str(order_data.get("status", "0"))]

            success_msg = (
                f"Successfully processed order status:\n"
                f"Trading pair: {tracked_order.trading_pair}\n"
                f"Order ID: {tracked_order.exchange_order_id}\n"
                f"Client Order ID: {tracked_order.client_order_id}\n"
                f"New state: {new_state}\n"
                f"Full order data: {order_data}"
            )
            print(success_msg, flush=True)
            self.logger().error(f"[ORDER STATUS] {success_msg}")
            sys.stdout.flush()
            sys.stderr.flush()

            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(order_data.get("orderId", "")),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=float(order_data.get("updateTime", 0)) * 1e-3,
                new_state=new_state,
            )

        except Exception as e:
            error_msg = (
                f"Error getting order status:\n"
                f"Trading pair: {tracked_order.trading_pair}\n"
                f"Order ID: {tracked_order.exchange_order_id}\n"
                f"Client Order ID: {tracked_order.client_order_id}\n"
                f"Error: {str(e)}"
            )
            print(f"ERROR: {error_msg}", flush=True)
            self.logger().error(f"[ORDER STATUS] {error_msg}", exc_info=True)
            sys.stdout.flush()
            sys.stderr.flush()
            raise

    async def _update_balances(self):
        """Update user balances."""
        try:
            # Get account info with balances
            account_info = await self._api_post(
                path_url=self._user_info_url,
                data={},  # Empty dict for required POST request
                is_auth_required=True
            )
            
            self.logger().debug(f"Account info response: {account_info}")
            
            # Clear existing balances
            self._account_available_balances.clear()
            self._account_balances.clear()
            
            # Handle LBank's updated balance format
            if isinstance(account_info, dict) and account_info.get("result") == "true" and "data" in account_info:
                for balance_entry in account_info["data"]:
                    asset = balance_entry.get("coin", "").upper()  # Get asset and convert to upper case
                    
                    # Convert string amounts to Decimal
                    available_amount = Decimal(str(balance_entry.get("usableAmt", "0")))
                    frozen_amount = Decimal(str(balance_entry.get("freezeAmt", "0")))
                    total_amount = Decimal(str(balance_entry.get("assetAmt", "0")))
                    
                    # Update the balance dictionaries
                    self._account_balances[asset] = total_amount
                    self._account_available_balances[asset] = available_amount
                    
                self.logger().info(f"Balances updated successfully for {len(self._account_balances)} assets")
            else:
                self.logger().error(f"Unexpected balance response format: {account_info}")
                raise ValueError("Failed to parse balance response")
            
        except Exception as e:
            self.logger().error(f"Could not update balances: {str(e)}", exc_info=True)
            raise

    def _generate_echostr(self, length: int = 35) -> str:
        """Generate a random echostr of specified length"""
        import random
        import string
        characters = string.ascii_letters + string.digits
        return ''.join(random.choice(characters) for _ in range(length))

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Fixed trading pair initialization
        """
        try:
            # Make sure we're accessing the correct data structure
            trading_pairs_data = exchange_info.get("data", [])
            if not trading_pairs_data:
                self.logger().error("No trading pairs data found in exchange info")
                return

            for symbol_data in trading_pairs_data:
                if not lbank_utils.is_exchange_information_valid(symbol_data):
                    continue
                    
                # LBank uses underscore separator, we need to convert to dash
                exchange_symbol = symbol_data["symbol"]  # e.g. "brg_usdt"
                trading_pair = exchange_symbol.replace("_", "-").upper()  # e.g. "BRG-USDT"
                
                # Store both mappings for bidirectional conversion
                self._trading_pair_symbol_map[trading_pair] = exchange_symbol
                self._symbol_trading_pair_map[exchange_symbol] = trading_pair
                
                self.logger().info(f"Initialized trading pair mapping: {trading_pair} -> {exchange_symbol}")

        except Exception as e:
            self.logger().error(f"Error initializing trading pair symbols: {str(e)}", exc_info=True)
            raise

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params
        )

        return float(resp_json["lastPrice"])

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """
        Satisfies the abstract method from ExchangePyBase and
        creates a data source (object) to retrieve order book data.

        Update "LbankAPIOrderBookDataSource" import/path accordingly
        if the class is in a different module.
        """
        return LbankAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain  # use or remove domain as needed
        )

    async def get_token_price_in_usdt(self, token: str) -> Optional[Decimal]:
        """
        Gets the USDT price for a specific token using LBank's price endpoint
        :param token: The token symbol (e.g., 'BTC', 'ETH')
        :return: Price in USDT if available, None otherwise
        """
        try:
            if token == "USDT":
                return Decimal("1.0")
            
            symbol = f"{token.lower()}_usdt"
            self.logger().info(f"Fetching price for {symbol}")
            
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PRICE_PATH_URL,
                params={"symbol": symbol}
            )
            
            self.logger().info(f"Raw price response for {symbol}: {response}")
            
            # Handle the actual response format from LBank
            if isinstance(response, dict):
                # Check if it's a successful response
                if response.get("result") is True and "data" in response:
                    price_data = response["data"][0]
                    if isinstance(price_data, dict) and "price" in price_data:
                        try:
                            price = Decimal(str(price_data["price"]))
                            self.logger().info(f"✓ Got price for {token}: {price} USDT")
                            return price
                        except (TypeError, ValueError) as e:
                            self.logger().error(f"Error converting price value for {token}: {e}")
                # Handle error response
                elif response.get("result") == "false" and "error_code" in response:
                    self.logger().info(f"Price not available for {token}: {response.get('msg')}")
                else:
                    self.logger().error(f"Unexpected response format: {response}")
            
            return None
        except Exception as e:
            self.logger().error(f"Error fetching price for {token}: {str(e)}")
            return None

    async def all_balances(self) -> str:
        """
        Ensures balances are updated before displaying them
        """
        await self._update_balances()
        return self.get_balance_display()

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """Convert a trading pair to exchange symbol format."""
        return lbank_utils.convert_to_exchange_trading_pair(trading_pair)

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        """Convert an exchange symbol to Hummingbot trading pair format."""
        return lbank_utils.convert_from_exchange_trading_pair(symbol)

    async def _api_post(
        self,
        path_url: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends a POST request to LBank API
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        
        if data is None:
            data = {}
            
        # Set required headers
        if headers is None:
            headers = {}
        headers.update({
            "Content-Type": "application/x-www-form-urlencoded",
        })
        
        try:
            # Get raw response first, similar to user stream data source
            raw_response = await rest_assistant.execute_request_and_get_response(
                url=self.web_utils.private_rest_url(path_url=path_url),
                data=data,
                params=params,
                headers=headers, 
                method=RESTMethod.POST,
                is_auth_required=is_auth_required,
                throttler_limit_id=limit_id if limit_id else path_url,
            )

            # Get the raw text first, like in LbankAPIUserStreamDataSource
            raw_text = await raw_response.text()
            self.logger().debug(f"Raw response text from {path_url}: {raw_text}")
            
            # Try to parse it as JSON
            try:
                parsed_response = json.loads(raw_text)
            except json.JSONDecodeError:
                self.logger().error(f"Non-JSON response from {path_url}: {raw_text}")
                raise ValueError(f"Invalid JSON response from {path_url}: {raw_text}")
            
            # Handle error responses according to docs
            if isinstance(parsed_response, dict):
                if not parsed_response.get("result", True):
                    error_msg = parsed_response.get("msg", "Unknown error")
                    error_code = parsed_response.get("error_code", "Unknown code") 
                    raise ValueError(f"LBank API error: {error_msg} (code: {error_code})")
                    
            return parsed_response
            
        except Exception as e:
            self.logger().error(f"Error making POST request to {path_url}: {str(e)}")
            self.logger().error(f"Request data: {data}")
            raise

    def _initialize_markets(self, market_names: List[Tuple[str, List[str]]]):
        """
        Initialize markets and ensure balances are properly set
        """
        self._trading_required = False
        
        # Initialize trading pairs
        for market_name, trading_pairs in market_names:
            if market_name not in self.market_trading_pairs_map:
                self.market_trading_pairs_map[market_name] = []
            for hb_trading_pair in trading_pairs:
                self.market_trading_pairs_map[market_name].append(hb_trading_pair)
                
                # Initialize balances for the trading pair tokens
                base, quote = self.split_trading_pair(hb_trading_pair)
                for token in [base, quote]:
                    if token not in self._account_balances:
                        self._account_balances[token] = s_decimal_0
                    if token not in self._account_available_balances:
                        self._account_available_balances[token] = s_decimal_0
        
        super()._initialize_markets(market_names)
        self._trading_rules = {}
        self._trading_required = True

    async def _update_order_status(self):
        """
        Only check order status if there are active orders and trading is required
        """
        if not self._trading_required:
            return
        
        active_orders = self._order_tracker.active_orders
        if not active_orders:
            self.logger().debug("No active orders. Skipping order status update.")
            return
        
        # ... rest of the order status update logic ...

    @property
    def ready(self) -> bool:
        """
        Checks if the connector is ready for trading.
        Simplified ready check that focuses on essential components.
        """
        ready = all([
            self._trading_pairs is not None,  # Trading pairs are initialized
            self._user_stream_tracker is not None,  # User stream tracker exists
            self._account_balances is not None,  # Balances are initialized
            not self._trading_required or len(self._account_balances) > 0,  # Either trading not required or has balance info
        ])
        return ready

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Decimal:
        order_book = self.get_order_book(trading_pair)
        if order_book is None:
            raise ValueError(f"Order book for {trading_pair} is not available.")
        
        # Convert both prices to Decimal before calculation
        best_bid = Decimal(str(order_book.get_price(True)))
        best_ask = Decimal(str(order_book.get_price(False)))
        mid_price = (best_bid + best_ask) / Decimal("2")
        
        # Return different price types based on the request
        if price_type == PriceType.MidPrice:
            return mid_price
        elif price_type == PriceType.BestBid:
            return best_bid
        elif price_type == PriceType.BestAsk:
            return best_ask
        else:
            raise ValueError(f"Unrecognized price type: {price_type}")

    def get_order_book(self, trading_pair: str) -> OrderBook:
        self.logger().info(f"[ORDER BOOK] Getting order book for {trading_pair}")
        self.logger().info(f"[ORDER BOOK] Available order books: {list(self.order_books.keys())}")
        
        if trading_pair not in self.order_books:
            self.logger().error(f"[ORDER BOOK] No order book exists for '{trading_pair}'")
            self.logger().info(f"[ORDER BOOK] Current order tracker state: {self._order_tracker.active_orders}")
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        
        order_book = self.order_books[trading_pair]
        if order_book is None:
            self.logger().error(f"[ORDER BOOK] Order book is None for {trading_pair}")
            raise ValueError(f"Order book is None for {trading_pair}")
        
        self.logger().info(f"[ORDER BOOK] Retrieved order book for {trading_pair}: {order_book}")
        return order_book

    async def _initialize_trading_pair_symbol_map(self):
        """
        Initialize the trading pair symbol map. This method is called by the base class's trading_pair_symbol_map() method.
        """
        try:
            # Get exchange info
            exchange_info = await self._api_get(
                path_url=self.trading_pairs_request_path,
            )
            
            # Initialize the bidict for trading pair mapping
            mapping_dict = bidict()
            
            # Process the exchange info
            trading_pairs_data = exchange_info.get("data", [])
            if not trading_pairs_data:
                self.logger().error("No trading pairs data found in exchange info")
                return
            
            for symbol_data in trading_pairs_data:
                if not lbank_utils.is_exchange_information_valid(symbol_data):
                    continue
                    
                # LBank uses underscore separator, we need to convert to dash
                exchange_symbol = symbol_data["symbol"]  # e.g. "brg_usdt"
                trading_pair = exchange_symbol.replace("_", "-").upper()  # e.g. "BRG-USDT"
                
                # Store in bidict
                mapping_dict[exchange_symbol] = trading_pair
                
                self.logger().info(f"Initialized trading pair mapping: {exchange_symbol} -> {trading_pair}")
            
            self._set_trading_pair_symbol_map(mapping_dict)

        except Exception as e:
            self.logger().error(f"Error initializing trading pair symbol map: {str(e)}", exc_info=True)
            raise

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all active orders by using the cancel_order_by_symbol endpoint for each trading pair.
        """
        cancellation_results = []
        try:
            tasks = []
            for trading_pair in self._trading_pairs:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
                self.logger().info(f"Requesting cancellation of all orders for {trading_pair} ({exchange_symbol})")
                
                # Create REST request for cancelling all orders for this symbol
                rest_assistant = await self._web_assistants_factory.get_rest_assistant()
                request = RESTRequest(
                    method=RESTMethod.POST,
                    url=web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDERS_BY_PAIR_PATH_URL),
                    data={"symbol": exchange_symbol},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    is_auth_required=True
                )
                tasks.append(rest_assistant.call(request))

            self.logger().info(f"Awaiting cancellation results for {len(tasks)} trading pairs...")
            async with timeout(timeout_seconds):
                responses = await safe_gather(*tasks, return_exceptions=True)
                
                for trading_pair, response in zip(self._trading_pairs, responses):
                    if isinstance(response, Exception):
                        self.logger().error(f"Error cancelling orders for {trading_pair}: {str(response)}")
                        cancellation_results.append(CancellationResult(f"all_orders_{trading_pair}", False))
                        continue
                        
                    response_json = await response.json()
                    self.logger().debug(f"Cancel all response for {trading_pair}: {response_json}")
                    
                    # LBank returns success even if there were no orders to cancel
                    # The response format is {"result": true, "data": {"success": true}}
                    if response_json.get("result") is True:
                        data = response_json.get("data", {})
                        if isinstance(data, dict) and data.get("success") is True:
                            self.logger().info(f"Successfully cancelled all orders for {trading_pair}")
                            cancellation_results.append(CancellationResult(f"all_orders_{trading_pair}", True))
                        else:
                            self.logger().info(f"No orders to cancel for {trading_pair}")
                            cancellation_results.append(CancellationResult(f"all_orders_{trading_pair}", True))
                    else:
                        error_msg = response_json.get("error_message", "Unknown error")
                        self.logger().warning(f"Cancel all response for {trading_pair} indicates failure: {error_msg}")
                        cancellation_results.append(CancellationResult(f"all_orders_{trading_pair}", False))

        except Exception as e:
            self.logger().error(f"Error cancelling all orders: {str(e)}", exc_info=True)
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel orders. Check API key and network connection."
            )

        # Log final summary
        successful = len([r for r in cancellation_results if r.success])
        failed = len([r for r in cancellation_results if not r.success])
        self.logger().info(f"Cancel all completed: {successful} successful, {failed} failed")
        return cancellation_results

    async def _execute_cancel(self, trading_pair: str, order_id: str) -> str:
        """
        Executes order cancellation for a specific order
        """
        try:
            tracked_order = self._order_tracker.active_orders.get(order_id)
            if tracked_order is None:
                self.logger().info(f"Order {order_id} not found in local tracker, checking exchange...")
                
                # Get exchange symbol
                exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
                
                # Check active orders on exchange
                rest_assistant = await self._web_assistants_factory.get_rest_assistant()
                request = RESTRequest(
                    method=RESTMethod.POST,
                    url=web_utils.private_rest_url(path_url="/v2/supplement/orders_info_no_deal.do"),
                    data={
                        "symbol": exchange_symbol,
                        "current_page": "1",
                        "page_length": "100"
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    is_auth_required=True
                )
                
                response = await rest_assistant.call(request)
                orders_data = await response.json()
                
                self.logger().info(f"Active orders from exchange for {trading_pair}: {orders_data}")
                
                # Try to find our order in the active orders
                if orders_data.get("result") is True and "data" in orders_data:
                    orders = orders_data["data"].get("orders", [])
                    for order in orders:
                        if order.get("custom_id") == order_id:
                            # Found our order, proceed with cancellation
                            self.logger().info(f"Found order {order_id} on exchange, proceeding with cancellation")
                            cancel_request = RESTRequest(
                                method=RESTMethod.POST,
                                url=web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_ORDER_PATH_URL),
                                data={
                                    "symbol": exchange_symbol,
                                    "order_id": order.get("order_id")
                                },
                                headers={"Content-Type": "application/x-www-form-urlencoded"},
                                is_auth_required=True
                            )
                            
                            cancel_response = await rest_assistant.call(cancel_request)
                            cancel_data = await cancel_response.json()
                            
                            if cancel_data.get("result") is True:
                                self.logger().info(f"Successfully cancelled order {order_id}")
                                return order_id
                            
                            self.logger().error(f"Failed to cancel order {order_id}: {cancel_data.get('msg', 'Unknown error')}")
                            return None
                
                self.logger().info(f"Order {order_id} not found in active orders on exchange")
                return None
            
            # If we have the tracked order, proceed with normal cancellation
            self.logger().info(f"Found order {order_id} in local tracker, proceeding with cancellation")
            cancelled = await self._place_cancel(order_id=order_id, tracked_order=tracked_order)
            
            if cancelled:
                self.logger().info(f"Successfully cancelled order {order_id}")
                return order_id
            else:
                self.logger().error(f"Failed to cancel order {order_id}")
                return None
                
        except Exception as e:
            self.logger().error(f"Error executing cancellation for order {order_id}: {str(e)}", exc_info=True)
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel order {order_id} on LBank. "
                              f"Check API key and network connection."
            )
        return None
