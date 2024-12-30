import asyncio
import hashlib
import hmac
import json
import logging
import random
import string
import time
from base64 import b64encode
from collections import OrderedDict
from dataclasses import replace
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class LbankAuth(AuthBase):
    """
    Auth class required by LBank API
    Learn more at https://www.lbank.com/docs/index.html#authentication
    """
    RSA_STR = "RSA"
    HMACSHA256_STR = "HMACSHA256"
    API_HMACSHA = "HmacSHA256"
    ALLOW_METHOD = [RSA_STR, HMACSHA256_STR]

    def __init__(self, 
                 api_key: str, 
                 secret_key: str, 
                 auth_method: Optional[str] = "HmacSHA256",
                 time_provider: Optional[Callable] = None) -> None:
        super().__init__()
        auth_method = auth_method.upper()
        if auth_method not in self.ALLOW_METHOD:
            raise ValueError(f"{auth_method} sign method is not supported!")
        if auth_method == self.HMACSHA256_STR:
            auth_method = self.API_HMACSHA
        self.api_key = api_key
        self.secret_key = secret_key
        self.auth_method = auth_method
        self._time_provider = time_provider or time.time

    def _generate_echostr(self) -> str:
        """Generates random echostr between 30-40 chars as required by LBank"""
        return "".join(random.choices(string.ascii_letters + string.digits, k=35))

    def _create_param_string(self, params: Dict[str, Any], timestamp: str, echostr: str) -> str:
        """
        Step 1: Create the parameter string that needs to be signed
        All parameters (except sign) must be sorted alphabetically
        """
        # Combine all parameters (including api_key, timestamp, signature_method, echostr)
        all_params = {
            "api_key": self.api_key,
            "echostr": echostr,
            "signature_method": self.API_HMACSHA,
            "timestamp": timestamp,
            **params
        }
        
        # Sort parameters alphabetically by key
        sorted_params = sorted(all_params.items())
        
        # Join parameters with = and &
        return "&".join([f"{key}={value}" for key, value in sorted_params])

    def _create_md5_digest(self, param_string: str) -> str:
        """
        Step 2: Create MD5 digest of the parameter string
        Must be hex encoded and uppercase
        """
        return hashlib.md5(param_string.encode("utf8")).hexdigest().upper()

    def build_hmacsha256(self, param_string: str) -> str:
        """
        Step 3: Create HmacSHA256 signature
        For HMACSHA256: use secret key to make the hash operation of preparedStr directly
        """
        # First get the preparedStr (MD5 digest)
        prepared_str = self._create_md5_digest(param_string)
        
        # Directly sign the preparedStr with HMAC-SHA256
        api_secret = bytes(self.secret_key, encoding="utf8")
        signature = hmac.new(
            api_secret,
            prepared_str.encode("utf8"),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        return signature

    def build_payload(self, params: Dict[str, Any], timestamp: str, echostr: str) -> Dict[str, Any]:
        """
        Build the complete payload following LBank's documentation
        """
        # Step 1: Create parameter string
        param_string = self._create_param_string(params, timestamp, echostr)
        
        # Step 2 & 3: Generate signature
        if self.auth_method == self.RSA_STR:
            signature = self.build_rsasignv2(param_string)
        elif self.auth_method == self.API_HMACSHA:
            signature = self.build_hmacsha256(param_string)
        else:
            raise ValueError(f"{self.auth_method} sign method is not supported!")

        # Create final request parameters
        request_params = {
            **params,  # Original parameters
            "api_key": self.api_key,
            "sign": signature
        }
        
        return request_params

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication information to REST request
        """
        # Get timestamp and echostr
        timestamp = str(int(self._time_provider() * 1000))
        echostr = self._generate_echostr()
        
        # Parse existing data
        data = {}
        if request.data is not None:
            if isinstance(request.data, str):
                pairs = request.data.split('&')
                data = {k: v for k, v in (pair.split('=') for pair in pairs if '=' in pair)}
            else:
                data = dict(request.data)

        # Build payload with signature
        request_params = self.build_payload(data, timestamp, echostr)
        
        # URL encode parameters
        encoded_data = urlencode(request_params)

        # Set headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "timestamp": timestamp,
            "signature_method": self.API_HMACSHA,
            "echostr": echostr
        }
        if request.headers is not None:
            headers.update(request.headers)

        return replace(request, data=encoded_data, headers=headers)

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """LBank websocket doesn't require authentication"""
        return request
