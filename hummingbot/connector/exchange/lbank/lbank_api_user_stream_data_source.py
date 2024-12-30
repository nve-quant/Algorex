import asyncio
import json
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.lbank import lbank_constants as CONSTANTS, lbank_web_utils as web_utils
from hummingbot.connector.exchange.lbank.lbank_auth import LbankAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from .lbank_exchange import LbankExchange


class LbankAPIUserStreamDataSource(UserStreamTrackerDataSource):
    # LBank requires ping every 15 seconds
    HEARTBEAT_TIME_INTERVAL = 15.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: LbankAuth,
        connector: "LbankExchange",
        api_factory: WebAssistantsFactory,
        trading_pairs: List[str]
    ):
        super().__init__()
        self._auth: LbankAuth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._trading_pairs = trading_pairs

        self._rest_assistant: Optional[RESTAssistant] = None
        self._ws_assistant: Optional[WSAssistant] = None
        self._current_listening_key: Optional[str] = None
        self._listen_key_initialized_event = asyncio.Event()
        self._last_listen_key_ping_ts: int = 0
        self._manage_listen_key_task = None
        
        # Add message queue
        self._user_stream: asyncio.Queue = asyncio.Queue()

    # Add property for user stream
    @property
    def user_stream(self) -> asyncio.Queue:
        return self._user_stream

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user stream websocket and listens for updates.
        """
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                
                async for ws_response in ws.iter_messages():
                    data = ws_response.data
                    await self._process_event_message(data, output)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(
                    f"Unexpected error while listening to user stream. Error: {str(e)}",
                    exc_info=True
                )
                await self._sleep(5.0)
            finally:
                # Always cleanup
                await self._on_user_stream_interruption(ws)
                self._manage_listen_key_task and self._manage_listen_key_task.cancel()
                await self._sleep(5.0)

    async def _get_listening_key(self) -> str:
        """
        Fetches a new listening key (subscribeKey) from LBank.
        """
        try:
            self.logger().info("[REST] Requesting new listening key...")
            
            # validate API key
            if not self._auth.api_key:
                raise ValueError("No LBank API key is set.")

            rest_assistant: RESTAssistant = await self._get_rest_assistant()

            params = {
                "api_key": self._auth.api_key,
                "timestamp": str(int(time.time() * 1000)),
                "sign_type": "2" if self._auth.auth_method == "RSA" else "1"
            }
            
            self.logger().debug(f"[REST] Listening key request params: {params}")
            
            raw_response = await rest_assistant.execute_request_and_get_response(
                url=web_utils.private_rest_url(
                    path_url=CONSTANTS.CREATE_LISTENING_KEY_PATH_URL,
                    domain=self._connector.domain
                ),
                method=RESTMethod.POST,
                data=params,
                is_auth_required=True,
                throttler_limit_id=CONSTANTS.CREATE_LISTENING_KEY_PATH_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )

            response_text = await raw_response.text()
            self.logger().debug(f"[REST] Listening key raw response: {response_text}")
            
            response = json.loads(response_text)
            
            if isinstance(response, dict):
                if response.get("result") == "false":
                    error_msg = response.get("msg", "Unknown error")
                    error_code = response.get("error_code", "Unknown code")
                    self.logger().error(f"[REST] Failed to get listening key: {error_msg} (code: {error_code})")
                    raise ValueError(f"LBank API error: {error_msg} (code: {error_code})")

                if "key" in response:
                    key = response["key"]
                    self.logger().info(f"[REST] Successfully obtained listening key: {key}")
                    return key
                elif "data" in response:
                    data_val = response["data"]
                    if isinstance(data_val, str):
                        self.logger().info(f"[REST] Successfully obtained listening key: {data_val}")
                        return data_val
                    elif isinstance(data_val, dict) and "key" in data_val:
                        key = data_val["key"]
                        self.logger().info(f"[REST] Successfully obtained listening key: {key}")
                        return key

            self.logger().error(f"[REST] Unexpected listening key response format: {response}")
            raise ValueError(f"No subscribe key in LBank response: {response}")

        except Exception as e:
            self.logger().error(f"[REST] Error getting listening key: {str(e)}", exc_info=True)
            raise

    def _generate_echostr(self) -> str:
        """
        Generates a random echostr as required by LBank API
        Must be 30-40 characters long, containing only digits and letters
        """
        import random
        import string
        length = random.randint(30, 40)
        characters = string.ascii_letters + string.digits
        return ''.join(random.choice(characters) for _ in range(length))

    async def _extend_listening_key(self) -> bool:
        """
        Extends validity of current listening key
        :return: True if the extension was successful, False otherwise
        """
        if self._current_listening_key is None:
            self.logger().warning("No listening key to extend...")
            return False

        try:
            rest_assistant: RESTAssistant = await self._get_rest_assistant()
            
            # Create parameters dictionary
            params = {
                "api_key": self._auth.api_key,
                "subscribeKey": self._current_listening_key,
                "timestamp": str(int(self._time() * 1000)),
            }
            
            # Sort parameters alphabetically and create signature
            sorted_params = dict(sorted(params.items()))
            signature = self._auth._generate_signature(sorted_params)
            sorted_params["sign"] = signature
            
            # Create the form data string
            import urllib.parse
            form_data = []
            for key, value in sorted_params.items():
                encoded_value = urllib.parse.quote(str(value))
                form_data.append(f"{key}={encoded_value}")
            form_data_str = "&".join(form_data)

            response = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(path_url=CONSTANTS.REFRESH_LISTENING_KEY_PATH_URL),
                method=RESTMethod.POST,
                data=form_data_str,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                throttler_limit_id=CONSTANTS.REFRESH_LISTENING_KEY_PATH_URL,
                is_auth_required=True,
            )

            if not isinstance(response, dict):
                self.logger().error(f"Unexpected response type: {type(response)}")
                return False

            if "error_code" in response:
                err_code = response.get("error_code", 0)
                err_msg = f"Error Code: {err_code} - {CONSTANTS.ERROR_CODES.get(err_code, '')}"
                self.logger().error(f"Error extending listening key: {err_msg}")
                return False

            # LBank returns {"result": true} for success
            return response.get("result") is True

        except Exception as e:
            self.logger().error(f"Error extending listening key: {str(e)}")
            return False

    async def _manage_listening_key_task_loop(self):
        """
        This method periodically extends the validity of the listen key.
        """
        try:
            while True:
                now = int(time.time())
                if self._current_listening_key is None:
                    self._current_listening_key = await self._get_listening_key()
                    self._last_listen_key_ping_ts = int(time.time())
                    self._listen_key_initialized_event.set()
                    continue

                if now - self._last_listen_key_ping_ts >= CONSTANTS.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    success: bool = await self._extend_listening_key()
                    if not success:
                        self.logger().warning("Failed to extend validity of the current listening key... Recreating listening key.")
                        self._current_listening_key = None
                        self._listen_key_initialized_event.clear()
                    else:
                        self.logger().info(f"Refreshed listen key {self._current_listening_key}.")
                        self._last_listen_key_ping_ts = int(time.time())

                await self._sleep(CONSTANTS.LISTEN_KEY_KEEP_ALIVE_INTERVAL)
        finally:
            self._current_listening_key = None
            self._listen_key_initialized_event.clear()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        self.logger().info("[WS] Connecting to user stream...")
        
        self._manage_listen_key_task = safe_ensure_future(self._manage_listening_key_task_loop())
        await self._listen_key_initialized_event.wait()
        
        ws: WSAssistant = await self._get_ws_assistant()
        self.logger().info(f"[WS] Connecting to {CONSTANTS.WSS_URL}")
        
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL,
            ping_timeout=self.HEARTBEAT_TIME_INTERVAL
        )
        
        self.logger().info("[WS] Connected to user stream")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to user balance and order update channels
        """
        try:
            # Subscribe to balance updates
            balance_payload = {
                "action": "subscribe",
                "subscribe": "assetUpdate",
                "subscribeKey": self._current_listening_key
            }
            self.logger().info(f"[WS] Subscribing to balance updates with payload: {balance_payload}")
            await websocket_assistant.send(WSJSONRequest(payload=balance_payload))

            # Subscribe to order updates for each trading pair
            for trading_pair in self._trading_pairs:
                order_payload = {
                    "action": "subscribe",
                    "subscribe": "orderUpdate",
                    "subscribeKey": self._current_listening_key,
                    "pair": await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                }
                self.logger().info(f"[WS] Subscribing to order updates with payload: {order_payload}")
                await websocket_assistant.send(WSJSONRequest(payload=order_payload))

            self.logger().info("[WS] Successfully subscribed to all private channels")
        except Exception as e:
            self.logger().error(f"[WS] Error subscribing to private channels: {str(e)}", exc_info=True)
            raise

    async def _handle_ping_message(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        """
        Handles ping messages from the exchange
        """
        try:
            pong_payload = {"action": "pong", "pong": event_message["ping"]}
            self.logger().debug(f"[WS] Sending pong response: {pong_payload}")
            await websocket_assistant.send(WSJSONRequest(payload=pong_payload))
        except Exception as e:
            self.logger().error(f"[WS] Error sending pong response: {str(e)}")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Process incoming messages from the user stream.
        """
        if len(event_message) > 0:
            self.logger().debug(f"[WS] Received message: {event_message}")
            
            # Handle ping messages
            if "ping" in event_message:
                self.logger().info(f"[WS] Received ping: {event_message['ping']}")
                ws: WSAssistant = await self._get_ws_assistant()
                await self._handle_ping_message(event_message, ws)
                self.logger().info("[WS] Sent pong response")
            
            # Handle actual updates
            elif "type" in event_message:
                channel = event_message["type"]
                if channel == CONSTANTS.USER_ORDER_UPDATE_CHANNEL:
                    self.logger().info(f"[WS] Received order update: {event_message}")
                    await queue.put(event_message)
                elif channel == CONSTANTS.USER_BALANCE_UPDATE_CHANNEL:
                    self.logger().info(f"[WS] Received balance update: {event_message}")
                    await queue.put(event_message)
                else:
                    self.logger().debug(f"[WS] Received message for unknown channel: {channel}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        self._manage_listen_key_task and self._manage_listen_key_task.cancel()
        self._current_listening_key = None
        self._listen_key_initialized_event.clear()
        await self._sleep(5.0)

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    def _ping_request_interval(self) -> float:
        return CONSTANTS.WS_PING_REQUEST_INTERVAL

    async def start(self):
        """
        Start the user stream data source
        """
        self.logger().info("Starting user stream...")
        self._manage_listen_key_task = safe_ensure_future(self._manage_listening_key_task_loop())
        
        # Initialize WebSocket connection
        ws: WSAssistant = await self._connected_websocket_assistant()
        
        # Subscribe to channels
        await self._subscribe_channels(ws)
        
        # Start message processing loop
        while True:
            try:
                async for message in ws.iter_messages():
                    if message is not None:
                        await self._process_event_message(message.data, self._user_stream)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error processing message: {str(e)}", exc_info=True)
                await self._sleep(5.0)
                await self.start()  # Restart on unexpected errors

    async def stop(self):
        """
        Stop the user stream data source
        """
        self.logger().info("Stopping user stream...")
        self._manage_listen_key_task and self._manage_listen_key_task.cancel()
        if self._ws_assistant is not None:
            await self._ws_assistant.disconnect()
            self._ws_assistant = None
        self._current_listening_key = None
        self._listen_key_initialized_event.clear()

    def _time(self) -> float:
        """
        Returns current timestamp in float
        """
        return time.time()
