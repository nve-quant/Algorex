import asyncio
import json
import time
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import dateutil.parser as date_parser

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS, lbank_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from .lbank_exchange import LbankExchange


class LbankAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, 
                 trading_pairs: List[str], 
                 connector: "LbankExchange", 
                 api_factory: WebAssistantsFactory,
                 domain: Optional[str] = None):
        """
        :param trading_pairs: a list of trading pairs
        :param connector: the connector instance
        :param api_factory: the web assistants factory instance
        :param domain: the domain to use (optional)
        """
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Creates an order book message from the order book snapshot.
        Handles empty responses and retries if needed.
        """
        max_retries = 3
        retry_delay = 5
        
        for retry_count in range(max_retries):
            try:
                snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
                self.logger().debug(f"Processing order book snapshot: {snapshot_response}")
                
                # Validate response structure
                if not isinstance(snapshot_response, dict):
                    raise ValueError(f"Invalid response format: {snapshot_response}")
                    
                # Check if response contains required data
                depth_data = snapshot_response.get("data", {})
                bids = depth_data.get("bids", [])
                asks = depth_data.get("asks", [])
                
                if not bids and not asks:
                    if retry_count < max_retries - 1:
                        self.logger().warning(
                            f"Empty order book snapshot received for {trading_pair}. "
                            f"Retrying {retry_count + 1}/{max_retries}..."
                        )
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        raise ValueError(f"Failed to get valid order book data after {max_retries} retries")
                
                # Convert timestamp to milliseconds if in seconds
                timestamp = float(snapshot_response.get("ts", time.time() * 1000))
                if timestamp < 1e12:  # If timestamp is in seconds
                    timestamp *= 1000
                    
                return OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content={
                        "trading_pair": trading_pair,
                        "update_id": timestamp,
                        "bids": [[float(price), float(amount)] for price, amount in bids],
                        "asks": [[float(price), float(amount)] for price, amount in asks],
                    },
                    timestamp=timestamp / 1000.0  # Convert to seconds for internal use
                )
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if retry_count == max_retries - 1:
                    self.logger().error(f"Failed to get order book snapshot for {trading_pair}. Error: {str(e)}")
                    raise
                else:
                    self.logger().warning(f"Error fetching order book snapshot: {str(e)}. Retrying...")
                    await asyncio.sleep(retry_delay)
                    continue

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Request full order book from the exchange, for a particular trading pair.
        """
        try:
            # Convert to exchange trading pair format (e.g. BTC-USDT -> btc_usdt)
            exchange_trading_pair = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
            
            # Updated params to match API requirements
            params = {
                "symbol": exchange_trading_pair,
                "size": CONSTANTS.ORDER_BOOK_SNAPSHOT_DEPTH  # Use constant for depth
            }

            self.logger().info(f"[ORDER BOOK] Requesting snapshot for {trading_pair} with payload: {params}")
            
            # Updated endpoint path to match documentation
            endpoint = "/v2/depth.do"  # Added leading slash
            
            # Use public_rest_url instead of get_rest_url_for_endpoint
            url = web_utils.public_rest_url(path_url=endpoint, domain=self._domain)
            self.logger().info(f"[ORDER BOOK] Making request to {url}")
            
            # Get REST assistant from API factory
            rest_assistant = await self._api_factory.get_rest_assistant()
            
            # Make the request with throttler_limit_id
            response = await rest_assistant.execute_request(
                url=url,
                params=params,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,  # Changed to use correct constant
            )
            
            # Add response logging for debugging
            self.logger().debug(f"[ORDER BOOK] Raw response: {response}")
            
            # Check for error response
            if isinstance(response, dict):
                if not response.get("result", True):
                    error_msg = response.get("msg", "Unknown error")
                    error_code = response.get("error_code", "Unknown code")
                    self.logger().error(f"[ORDER BOOK] API error: {error_msg} (code: {error_code})")
                    raise ValueError(f"API error: {error_msg} (code: {error_code})")
            
            return response
            
        except Exception as e:
            self.logger().error(f"[ORDER BOOK] Error requesting snapshot: {str(e)}")
            raise

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # The incrdepth channel in LBank is not sending updates consistently. The support team suggested to not use it
        # Instead the current implementation will register and use  the full updates channel

        raise NotImplementedError

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse order book snapshot message from websocket
        """
        try:
            trading_pair: str = await self._connector.trading_pair_associated_to_exchange_symbol(raw_message["pair"])
            timestamp: float = date_parser.parse(raw_message["TS"]).timestamp()
            update: Dict[str, Any] = raw_message[CONSTANTS.ORDER_BOOK_DEPTH_CHANNEL]
            update_id: int = int(timestamp * 1e3)

            # Convert and validate the data
            bids = [(float(bid[0]), float(bid[1])) for bid in update.get("bids", [])]
            asks = [(float(ask[0]), float(ask[1])) for ask in update.get("asks", [])]

            if not bids or not asks:
                self.logger().warning(f"Received empty order book data for {trading_pair}")
                return

            depth_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": sorted(bids, key=lambda x: float(x[0]), reverse=True),  # Sort bids in descending order
                "asks": sorted(asks, key=lambda x: float(x[0])),  # Sort asks in ascending order
            }

            message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                depth_message_content,
                timestamp
            )

            # Log the parsed message for debugging
            self.logger().debug(f"Parsed order book message for {trading_pair}: {depth_message_content}")
            
            await message_queue.put(message)

        except Exception as e:
            self.logger().error(f"Error parsing order book snapshot: {str(e)}", exc_info=True)
            raise

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse a trade message from the websocket stream and put it into the trade message queue
        """
        try:
            if raw_message.get("type") == "trade":
                trade_data = raw_message.get("data", [])
                if not isinstance(trade_data, list):
                    self.logger().error(f"Invalid trade data format: {trade_data}")
                    return
                
                for trade in trade_data:
                    try:
                        trading_pair = trade["pair"].replace("_", "-").upper()
                        timestamp = int(trade.get("TS", int(time.time() * 1000)))
                        price = Decimal(str(trade.get("price", "0")))
                        amount = Decimal(str(trade.get("amount", "0")))
                        trade_type = "sell" if trade.get("direction", "").lower() == "sell" else "buy"
                        
                        trade_message = OrderBookMessage(
                            message_type=OrderBookMessageType.TRADE,
                            content={
                                "trading_pair": trading_pair,
                                "trade_type": trade_type,
                                "trade_id": trade.get("id", str(timestamp)),
                                "update_id": timestamp,
                                "price": price,
                                "amount": amount,
                                "timestamp": timestamp * 1e-3,  # Convert to seconds
                            }
                        )
                        
                        await message_queue.put(trade_message)
                        
                    except Exception as e:
                        self.logger().error(f"Error parsing individual trade: {str(e)}", exc_info=True)
                        continue
                        
        except Exception as e:
            self.logger().error(f"Error parsing trade message: {str(e)}", exc_info=True)
            raise

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to order book and trade channels with proper error handling
        """
        try:
            for trading_pair in self._trading_pairs:
                # Convert trading pair format (e.g., BRG-USDT to brg_usdt)
                symbol: str = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                symbol = symbol.lower().replace('-', '_')
                
                # Subscribe to order book
                orderbook_payload = {
                    "action": "subscribe",
                    "subscribe": "depth",
                    "depth": str(CONSTANTS.ORDER_BOOK_DEPTH_CHANNEL_DEPTH),
                    "pair": symbol
                }
                
                self.logger().info(f"[WS] Subscribing to order book with payload: {orderbook_payload}")
                
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=orderbook_payload)
                await ws.send(subscribe_orderbook_request)
                
                # Log subscription response
                response = await ws.receive()
                self.logger().info(f"[WS] Order book subscription response: {response}")
                
                # Add delay between subscriptions
                await asyncio.sleep(0.5)
                
                # Subscribe to trades
                trade_payload = {
                    "action": "subscribe",
                    "subscribe": CONSTANTS.ORDER_BOOK_TRADE_CHANNEL,
                    "pair": symbol
                }
                
                self.logger().info(f"[WS] Subscribing to trades with payload: {trade_payload}")
                
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)
                await ws.send(subscribe_trade_request)
                
                # Log subscription response
                response = await ws.receive()
                self.logger().info(f"[WS] Trade subscription response: {response}")

            self.logger().info("[WS] Subscribed to public order book and trade channels...")
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"[WS] Error subscribing to channels: {str(e)}", exc_info=True)
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "ping" in event_message:
            channel = CONSTANTS.PING_RESPONSE
        if "type" in event_message:
            event_channel = event_message["type"]
            if event_channel == CONSTANTS.ORDER_BOOK_TRADE_CHANNEL:
                channel = self._trade_messages_queue_key
            if event_channel == CONSTANTS.ORDER_BOOK_DEPTH_CHANNEL:
                channel = self._snapshot_messages_queue_key

        return channel

    async def _handle_ping_message(self, event_message: Dict[str, Any], ws_assistant: WSAssistant):
        try:
            pong_payload = {"action": "pong", "pong": event_message["ping"]}
            pong_request: WSJSONRequest = WSJSONRequest(payload=pong_payload)
            await ws_assistant.send(pong_request)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Unexpected error occurred sending pong response to public stream connection... Error: {str(e)}"
            )

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        try:
            while True:
                try:
                    await asyncio.wait_for(
                        super()._process_websocket_messages(websocket_assistant=websocket_assistant),
                        timeout=self._ping_request_interval())
                except asyncio.TimeoutError:
                    payload = {
                        "action": "ping",
                        "ping": str(uuid.uuid4())
                    }
                    ping_request: WSJSONRequest = WSJSONRequest(payload=payload)
                    await websocket_assistant.send(ping_request)
        except ConnectionError as e:
            if "Close code = 1000" in str(e):  # WS closed by server
                self.logger().warning(str(e))
            else:
                raise

    async def _process_message_for_unknown_channel(
            self,
            event_message: Dict[str, Any],
            websocket_assistant: WSAssistant):
        if "ping" in event_message:
            await self._handle_ping_message(event_message=event_message, ws_assistant=websocket_assistant)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL)
        return ws

    def _ping_request_interval(self):
        return CONSTANTS.WS_PING_REQUEST_INTERVAL

    def _parse_trade_message(self, trade_msg: Dict[str, Any], timestamp: float) -> OrderBookMessage:
        # Add error handling
        try:
            trading_pair = trade_msg["pair"]
            trade_type = float(TradeType.BUY if trade_msg["direction"] == "buy" else TradeType.SELL)
            price = float(trade_msg["price"])
            amount = float(trade_msg["volume"])
            trade_id = trade_msg["TS"]
        except (KeyError, ValueError) as e:
            self.logger().error(f"Error parsing trade message: {e}")
            return None
        
        # ... rest of the code ...
