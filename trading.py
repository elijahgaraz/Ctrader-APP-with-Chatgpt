from __future__ import annotations
import threading
import webbrowser
import requests
import random
import time
from collections import deque
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAGetTrendbarsReq
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod
from typing import List, Any, Optional, Tuple, Dict
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
import queue
import sys
import traceback
import pandas as pd
from datetime import datetime, timezone
from dataclasses import dataclass

@dataclass
class Position:
    """Represents an open trading position."""
    position_id: int
    symbol_name: str
    trade_side: str  # "BUY" or "SELL"
    volume_lots: float
    open_price: float
    open_timestamp: int
    current_pnl: float = 0.0

@dataclass
class AiAdvice:
    """Represents the advice received from the AI Overseer."""
    action: str
    confidence: float
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None
    reason: Optional[str] = None

TREND_BAR_PERIOD_SECONDS = {
    ProtoOATrendbarPeriod.M1: 60,
    ProtoOATrendbarPeriod.M5: 300,
    ProtoOATrendbarPeriod.M15: 900,
    ProtoOATrendbarPeriod.M30: 1800,
    ProtoOATrendbarPeriod.H1: 3600,
    ProtoOATrendbarPeriod.H4: 14400,
    ProtoOATrendbarPeriod.D1: 86400,
    ProtoOATrendbarPeriod.W1: 604800,
}

# Conditional import for Twisted reactor for GUI integration
_reactor_installed = False
try:
    from twisted.internet import reactor, tksupport
    _reactor_installed = True
except ImportError:
    print("Twisted reactor or GUI support not found. GUI integration with Twisted might require manual setup.")


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, auth_code_queue: queue.Queue, **kwargs):
        self.auth_code_queue = auth_code_queue
        super().__init__(*args, **kwargs)

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == "/callback":
            query_components = urllib.parse.parse_qs(parsed_path.query)
            auth_code = query_components.get("code", [None])[0]

            if auth_code:
                self.auth_code_queue.put(auth_code)
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authentication Successful!</h1>")
                self.wfile.write(b"<p>You can close this browser tab and return to the application.</p></body></html>")
                print(f"OAuth callback handled, code extracted: {auth_code[:20]}...")
            else:
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authentication Failed</h1><p>No authorization code found in callback.</p></body></html>")
                print("OAuth callback error: No authorization code found.")
                self.auth_code_queue.put(None) # Signal failure or no code
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Not Found</h1></body></html>")

    def log_message(self, format, *args):
        # Suppress most log messages from the HTTP server for cleaner console
        # You might want to enable some for debugging.
        # Example: only log errors or specific messages
        if "400" in args[0] or "404" in args[0] or "code 200" in args[0]: # Log errors and successful callback
             super().log_message(format, *args)
        # else:
        #    pass


import json # For token persistence

# Imports from ctrader-open-api
try:
    from ctrader_open_api import Client, TcpProtocol, EndPoints, Protobuf
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
        ProtoHeartbeatEvent,
        ProtoErrorRes,
        ProtoMessage
        # ProtoPayloadType / ProtoOAPayloadType not here
    )
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
        ProtoOATraderReq, ProtoOATraderRes,
        ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes,
        ProtoOASpotEvent, ProtoOATraderUpdatedEvent,
        ProtoOANewOrderReq, ProtoOAExecutionEvent,
        ProtoOAOrderErrorEvent,
        ProtoOAErrorRes,
        # Specific message types for deserialization
        ProtoOAGetCtidProfileByTokenRes,
        ProtoOAGetCtidProfileByTokenReq,
        ProtoOASymbolsListReq, ProtoOASymbolsListRes, # For fetching symbol list (light symbols)
        ProtoOASymbolByIdReq, ProtoOASymbolByIdRes,    # For fetching full symbol details
        ProtoOAClosePositionReq, # For closing positions
        ProtoOAGetTrendbarsReq, # Added for historical data
        ProtoOAGetTrendbarsRes  # Added for historical data
    )
    # ProtoOALightSymbol is implicitly used by ProtoOASymbolsListRes
    # ProtoOASymbol is used by ProtoOASymbolByIdRes and for our symbol_details_map value type
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
        ProtoOATrader, ProtoOASymbol,
        ProtoOAOrderType,      # Moved for place_market_order
        ProtoOATradeSide,      # Moved for place_market_order
        ProtoOAExecutionType,  # Moved for _handle_execution_event
        ProtoOAOrderStatus,     # Moved for _handle_execution_event
        ProtoOATrendbarPeriod,  # Added for historical data
        ProtoOAPositionStatus
    )
    USE_OPENAPI_LIB = True
except ImportError as e:
    print(f"ctrader-open-api import failed ({e}); running in mock mode.")
    USE_OPENAPI_LIB = False

TOKEN_FILE_PATH = "tokens.json"

from typing import Callable

class Trader:
    def __init__(self, settings, history_size: int = 100, on_account_update: Optional[Callable[[Dict[str, Any]], None]] = None, on_positions_update: Optional[Callable[[Dict[int, Position]], None]] = None, on_log_message: Optional[Callable[[str], None]] = None):
        """
        Initializes the Trader.

        Args:
            settings: The application settings object.
            history_size: The maximum size of the price history to maintain.
            on_account_update: An optional callback function to be invoked with account summary updates.
            on_positions_update: An optional callback function to be invoked with position updates.
            on_log_message: An optional callback function for logging messages to the GUI.
        """
        self.settings = settings
        self.on_account_update = on_account_update
        self.on_positions_update = on_positions_update
        self.on_log_message = on_log_message
        self.is_connected: bool = False
        self._is_client_connected: bool = False
        self._last_error: str = ""
        self.latest_prices: Dict[str, float] = {}
        self.price_histories: Dict[str, deque] = {}
        self.history_size = history_size

        # OHLC Data Storage
        self.timeframes_seconds = {
            '15s': 15,
            '1m': 60,
            '5m': 300
        }
        self.current_bars: Dict[str, Dict[str, Dict]] = {} # symbol -> timeframe -> bar
        self.ohlc_history: Dict[str, Dict[str, pd.DataFrame]] = {} # symbol -> timeframe -> df
        self.max_ohlc_history_len = 500 # Max number of OHLC bars to keep per timeframe


        # Initialize token fields before loading
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None
        self._load_tokens_from_file() # Load tokens on initialization

        # Account details
        self.ctid_trader_account_id: Optional[int] = settings.openapi.default_ctid_trader_account_id
        self.account_id: Optional[str] = None # Will be string representation of ctidTraderAccountId
        self.balance: Optional[float] = None
        self.equity: Optional[float] = None
        self.currency: Optional[str] = None
        self.used_margin: Optional[float] = None
        self.free_margin: Optional[float] = None
        self.margin_level: Optional[float] = None

        # Position data
        self.open_positions: Dict[int, Position] = {}
        self._equity_update_thread = None
        self._stop_equity_updater = threading.Event()

        # Symbol data
        self.symbols_map: Dict[str, int] = {} # Map from symbol name to symbolId
        self.symbol_id_to_name_map: Dict[int, str] = {}
        self.symbol_details_map: Dict[int, Any] = {} # Map from symbolId to ProtoOASymbol
        self.default_symbol_id: Optional[int] = None # Symbol ID for the default_symbol from settings
        self.subscribed_spot_symbol_ids: set[int] = set()


        self._client: Optional[Client] = None
        self._message_id_counter: int = 1
        self._reactor_thread: Optional[threading.Thread] = None
        self._auth_code: Optional[str] = None # To store the auth code from OAuth flow
        self._account_auth_initiated: bool = False # Flag to prevent duplicate account auth attempts

        if USE_OPENAPI_LIB:
            host = (
                EndPoints.PROTOBUF_LIVE_HOST
                if settings.openapi.host_type == "live"
                else EndPoints.PROTOBUF_DEMO_HOST
            )
            port = EndPoints.PROTOBUF_PORT
            self._client = Client(host, port, TcpProtocol)
            self._client.setConnectedCallback(self._on_client_connected)
            self._client.setDisconnectedCallback(self._on_client_disconnected)
            self._client.setMessageReceivedCallback(self._on_message_received)
        else:
            print("Trader initialized in MOCK mode.")

        self._auth_code_queue = queue.Queue() # Queue to pass auth_code from HTTP server thread
        self._http_server_thread: Optional[threading.Thread] = None
        self._http_server: Optional[HTTPServer] = None

    def _save_tokens_to_file(self):
        """Saves the current OAuth access token, refresh token, and expiry time to TOKEN_FILE_PATH."""
        tokens = {
            "access_token": self._access_token,
            "refresh_token": self._refresh_token,
            "token_expires_at": self._token_expires_at,
        }
        try:
            with open(TOKEN_FILE_PATH, "w") as f:
                json.dump(tokens, f)
            print(f"Tokens saved to {TOKEN_FILE_PATH}")
        except IOError as e:
            print(f"Error saving tokens to {TOKEN_FILE_PATH}: {e}")

    def _load_tokens_from_file(self):
        """Loads OAuth tokens from a local file."""
        try:
            with open(TOKEN_FILE_PATH, "r") as f:
                tokens = json.load(f)
            self._access_token = tokens.get("access_token")
            self._refresh_token = tokens.get("refresh_token")
            self._token_expires_at = tokens.get("token_expires_at")
            if self._access_token:
                print(f"Tokens loaded from {TOKEN_FILE_PATH}. Access token: {self._access_token[:20]}...")
            else:
                print(f"{TOKEN_FILE_PATH} not found or no access token in it. Will need OAuth.")
        except FileNotFoundError:
            print(f"Token file {TOKEN_FILE_PATH} not found. New OAuth flow will be required.")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading tokens from {TOKEN_FILE_PATH}: {e}. Will need OAuth flow.")
            # In case of corrupted file, good to try to remove it or back it up
            try:
                import os
                os.remove(TOKEN_FILE_PATH)
                print(f"Removed corrupted token file: {TOKEN_FILE_PATH}")
            except OSError as rm_err:
                print(f"Error removing corrupted token file: {rm_err}")

    def _next_message_id(self) -> str:
        mid = str(self._message_id_counter)
        self._message_id_counter += 1
        return mid

    # Twisted callbacks
    def _on_client_connected(self, client: Client) -> None:
        print("OpenAPI Client Connected.")
        self._is_client_connected = True
        self._last_error = ""
        req = ProtoOAApplicationAuthReq()
        req.clientId = self.settings.openapi.client_id or ""
        req.clientSecret = self.settings.openapi.client_secret or ""
        if not req.clientId or not req.clientSecret:
            print("Missing OpenAPI credentials.")
            client.stopService()
            return
        print(f"Sending ProtoOAApplicationAuthReq: {req}")
        d = client.send(req)
        d.addCallbacks(self._handle_app_auth_response, self._handle_send_error)

    def _on_client_disconnected(self, client: Client, reason: Any) -> None:
        print(f"OpenAPI Client Disconnected: {reason}")
        self.is_connected = False
        self._is_client_connected = False
        self._account_auth_initiated = False # Reset flag

    def _on_message_received(self, client: Client, message: Any) -> None:
        print(f"Original message received (type: {type(message)}): {message}")

        # Attempt to extract and deserialize using Protobuf.extract
        try:
            actual_message = Protobuf.extract(message)
            print(f"Message extracted via Protobuf.extract (type: {type(actual_message)}): {actual_message}")
        except Exception as e:
            print(f"Error using Protobuf.extract: {e}. Falling back to manual deserialization if possible.")
            actual_message = message # Fallback to original message for manual processing attempt
            # Log additional details about the original message if it's a ProtoMessage
            if isinstance(message, ProtoMessage):
                 print(f"  Fallback: Original ProtoMessage PayloadType: {message.payloadType}, Payload Bytes: {message.payload[:64]}...") # Log first 64 bytes

        # If Protobuf.extract returned the original ProtoMessage wrapper, it means it couldn't deserialize it.
        # Or if an error occurred and we fell back.
        # We can attempt manual deserialization as before, but it's better if Protobuf.extract handles it.
        # For now, the dispatch logic below will use the result of Protobuf.extract.
        # If actual_message is still ProtoMessage, the specific isinstance checks will fail,
        # which is the correct behavior if it couldn't be properly deserialized.

        # We need to get payload_type for logging in case it's an unhandled ProtoMessage
        payload_type = 0
        if isinstance(actual_message, ProtoMessage): # If still a wrapper after extract attempt
            payload_type = actual_message.payloadType
            print(f"  Protobuf.extract did not fully deserialize. Message is still ProtoMessage wrapper with PayloadType: {payload_type}")
        elif isinstance(message, ProtoMessage) and actual_message is message: # Fallback case where actual_message was reset to original
            payload_type = message.payloadType
        # Ensure payload_type is defined for the final log message if it's an unhandled ProtoMessage
        final_payload_type_for_log = payload_type if isinstance(actual_message, ProtoMessage) else getattr(actual_message, 'payloadType', 'N/A')


        # Dispatch by type using the (potentially deserialized) actual_message
        if isinstance(actual_message, ProtoOAApplicationAuthRes):
            print("  Dispatching to _handle_app_auth_response")
            self._handle_app_auth_response(actual_message)
        elif isinstance(actual_message, ProtoOAAccountAuthRes):
            print("  Dispatching to _handle_account_auth_response")
            self._handle_account_auth_response(actual_message)
        elif isinstance(actual_message, ProtoOAGetCtidProfileByTokenRes):
            print("  Dispatching to _handle_get_ctid_profile_response")
            self._handle_get_ctid_profile_response(actual_message)
        elif isinstance(actual_message, ProtoOAGetAccountListByAccessTokenRes):
            print("  Dispatching to _handle_get_account_list_response")
            self._handle_get_account_list_response(actual_message)
        elif isinstance(actual_message, ProtoOASymbolsListRes):
            print("  Dispatching to _handle_symbols_list_response")
            self._handle_symbols_list_response(actual_message)
        elif isinstance(actual_message, ProtoOASymbolByIdRes):
            print("  Dispatching to _handle_symbol_details_response")
            self._handle_symbol_details_response(actual_message)
        elif isinstance(actual_message, ProtoOASubscribeSpotsRes):
            # This is usually handled by the callback in _send_subscribe_spots_request directly,
            # but good to have a dispatch log if it comes through _on_message_received.
            print("  Received ProtoOASubscribeSpotsRes (typically handled by send callback).")
            # self._handle_subscribe_spots_response(actual_message, []) # Might need context if called here
        elif isinstance(actual_message, ProtoOATraderRes):
            print("  Dispatching to _handle_trader_response")
            self._handle_trader_response(actual_message)
        elif isinstance(actual_message, ProtoOATraderUpdatedEvent):
            print("  Dispatching to _handle_trader_updated_event")
            self._handle_trader_updated_event(actual_message)
        elif isinstance(actual_message, ProtoOASpotEvent):
            self._handle_spot_event(actual_message) # Potentially noisy
            # print("  Received ProtoOASpotEvent (handler commented out).")
        elif isinstance(actual_message, ProtoOAExecutionEvent):
            self._handle_execution_event(actual_message)
            # print("  Received ProtoOAExecutionEvent (handler commented out).")
        elif isinstance(actual_message, ProtoOAGetTrendbarsRes):
            print("  Dispatching to _handle_get_trendbars_response")
            self._handle_get_trendbars_response(actual_message)
        elif isinstance(actual_message, ProtoOAOrderErrorEvent):
            error_message = f"[Order Error] {actual_message.errorCode}: {actual_message.description}"
            print(error_message)
            if self.on_log_message:
                self.on_log_message(error_message)
        elif isinstance(actual_message, ProtoHeartbeatEvent):
            print("  Received heartbeat.")
        elif isinstance(actual_message, ProtoOAErrorRes): # Specific OA error
            print(f"  Dispatching to ProtoOAErrorRes handler. Error code: {actual_message.errorCode}, Description: {actual_message.description}")
            self._last_error = f"{actual_message.errorCode}: {actual_message.description}"
            if "NOT_AUTHENTICATED" in actual_message.errorCode:
                self._last_error += ". Please reconnect."
                self.disconnect()
        elif isinstance(actual_message, ProtoErrorRes): # Common error
            print(f"  Dispatching to ProtoErrorRes (common) handler. Error code: {actual_message.errorCode}, Description: {actual_message.description}")
            self._last_error = f"Common Error {actual_message.errorCode}: {actual_message.description}"
            if "NOT_AUTHENTICATED" in actual_message.errorCode:
                self._last_error += ". Please reconnect."
                self.disconnect()
        # Check if it's still the ProtoMessage wrapper (meaning Protobuf.extract didn't deserialize it further)
        elif isinstance(actual_message, ProtoMessage): # Covers actual_message is message (if message was ProtoMessage)
                                                       # and actual_message is the result of extract but still a wrapper.
            print(f"  ProtoMessage with PayloadType {actual_message.payloadType} was not handled by specific type checks.")
        elif actual_message is message and not isinstance(message, ProtoMessage): # Original message was not ProtoMessage and not handled
             print(f"  Unhandled non-ProtoMessage type in _on_message_received: {type(actual_message)}")
        else: # Should ideally not be reached if all cases are handled
            print(f"  Message of type {type(actual_message)} (PayloadType {final_payload_type_for_log}) fell through all handlers.")

    # Handlers
    def _handle_app_auth_response(self, response: ProtoOAApplicationAuthRes) -> None:
        print("ApplicationAuth response received.")

        if self._account_auth_initiated:
            print("Account authentication process already initiated, skipping duplicate _handle_app_auth_response.")
            return

        # The access token from ProtoOAApplicationAuthRes is for the application's session.
        # We have a user-specific OAuth access token in self._access_token (if OAuth flow completed).
        # We should not overwrite self._access_token here if it was set by OAuth.
        # For ProtoOAAccountAuthReq, we must use the user's OAuth token.

        # Let's see if the response contains an access token, though we might not use it directly
        # if our main OAuth token is already present.
        app_session_token = getattr(response, 'accessToken', None)
        if app_session_token:
            print(f"ApplicationAuthRes provided an app session token: {app_session_token[:20]}...")
            # If self._access_token (OAuth user token) is NOT set,
            # this could be a fallback or an alternative flow not fully explored.
            # For now, we prioritize the OAuth token set by exchange_code_for_token.
            if not self._access_token:
                print("Warning: OAuth access token not found, but AppAuthRes provided one. This scenario needs review.")
                # self._access_token = app_session_token # Potentially use if no OAuth token? Risky.

        if not self._access_token:
            self._last_error = "Critical: OAuth access token not available for subsequent account operations."
            print(self._last_error)
            # Potentially stop the client or signal a critical failure here
            if self._client:
                self._client.stopService()
            return

        # Proceed to account authentication or discovery, using the OAuth access token (self._access_token)
        if self.ctid_trader_account_id and self._access_token:
            # If a ctidTraderAccountId is known (e.g., from settings) and we have an OAuth access token,
            # proceed directly with ProtoOAAccountAuthReq as per standard Spotware flow.
            print(f"Known ctidTraderAccountId: {self.ctid_trader_account_id}. Attempting ProtoOAAccountAuthReq.")
            self._account_auth_initiated = True # Set flag before sending
            self._send_account_auth_request(self.ctid_trader_account_id)
        elif self._access_token:
            # If ctidTraderAccountId is not known, but we have an access token,
            # first try to get the account list associated with this token.
            # ProtoOAGetAccountListByAccessTokenReq is preferred over ProtoOAGetCtidProfileByTokenReq
            # if the goal is to find trading accounts. Profile is more about user details.
            print("No default ctidTraderAccountId. Attempting to get account list by access token.")
            self._account_auth_initiated = True # Set flag before sending
            self._send_get_account_list_request()
        else:
            # This case should ideally be prevented by earlier checks in the connect() flow.
            self._last_error = "Critical: Cannot proceed with account auth/discovery. Missing ctidTraderAccountId or access token after app auth."
            print(self._last_error)
            if self._client:
                self._client.stopService()


    def _handle_get_ctid_profile_response(self, response: ProtoOAGetCtidProfileByTokenRes) -> None:
        """
        Handles the response from a ProtoOAGetCtidProfileByTokenReq.
        Its primary role is to provide user profile information.
        It might also list associated ctidTraderAccountIds, which can be used if an ID isn't already known.
        This handler does NOT set self.is_connected; connection is confirmed by ProtoOAAccountAuthRes.
        """
        print(f"Received ProtoOAGetCtidProfileByTokenRes. Content: {response}")

        # Example of how you might use profile data if needed:
        # if hasattr(response, 'profile') and response.profile:
        #     print(f"  User Profile Nickname: {response.profile.nickname}")

        # Check if the response contains ctidTraderAccount details.
        # According to some message definitions, ProtoOAGetCtidProfileByTokenRes
        # might not directly list accounts. ProtoOAGetAccountListByAccessTokenRes is for that.
        # However, if it *does* provide an account ID and we don't have one, we could note it.
        # For now, this handler mainly logs. If a ctidTraderAccountId is needed and not present,
        # the flow should have gone through _send_get_account_list_request.

        # If, for some reason, this response is used to discover a ctidTraderAccountId:
        # found_ctid = None
        # if hasattr(response, 'ctidProfile') and hasattr(response.ctidProfile, 'ctidTraderId'): # Speculative
        #     found_ctid = response.ctidProfile.ctidTraderId
        #
        # if found_ctid and not self.ctid_trader_account_id:
        #     print(f"  Discovered ctidTraderAccountId from profile: {found_ctid}. Will attempt account auth.")
        #     self.ctid_trader_account_id = found_ctid
        #     self._send_account_auth_request(self.ctid_trader_account_id)
        # elif not self.ctid_trader_account_id:
        #     self._last_error = "Profile received, but no ctidTraderAccountId found to proceed with account auth."
        #     print(self._last_error)

        # This response does not confirm a live trading session for an account.
        # That's the role of ProtoOAAccountAuthRes.
        pass

    def _handle_subscribe_spots_response(self, response_wrapper: Any, subscribed_symbol_ids: List[int]) -> None:
        """Handles the response from a ProtoOASubscribeSpotsReq."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_subscribe_spots_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASubscribeSpotsRes):
            print(f"_handle_subscribe_spots_response: Expected ProtoOASubscribeSpotsRes, got {type(actual_message)}. Message: {actual_message}")
            # Potentially set an error or log failure for specific symbols if the response structure allowed it.
            # For ProtoOASubscribeSpotsRes, it's usually an empty message on success.
            # Errors would typically come as ProtoOAErrorRes or via _handle_send_error.
            self._last_error = f"Spot subscription response was not ProtoOASubscribeSpotsRes for symbols {subscribed_symbol_ids}."
            return

        # ProtoOASubscribeSpotsRes is an empty message. Its reception confirms the subscription request was processed.
        # Actual spot data will come via ProtoOASpotEvent.
        print(f"Successfully processed spot subscription request for ctidTraderAccountId: {self.ctid_trader_account_id} and symbol IDs: {subscribed_symbol_ids}.")
        # No specific action needed here other than logging, errors are usually separate messages.

    def _send_get_symbol_details_request(self, symbol_ids: List[int]) -> None:
        """Sends a ProtoOASymbolByIdReq to get full details for specific symbol IDs."""
        if not self._ensure_valid_token():
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot get symbol details: Client not connected."
            print(self._last_error)
            return
        if not self.ctid_trader_account_id: # ctidTraderAccountId is not part of ProtoOASymbolByIdReq
            pass # but good to ensure we have it generally for consistency
        if not symbol_ids:
            self._last_error = "Cannot get symbol details: No symbol_ids provided."
            print(self._last_error)
            return

        print(f"Requesting full symbol details for IDs: {symbol_ids}")
        req = ProtoOASymbolByIdReq()
        req.symbolId.extend(symbol_ids)
        if not self.ctid_trader_account_id:
            self._last_error = "Cannot get symbol details: ctidTraderAccountId is not set."
            print(self._last_error)
            return
        req.ctidTraderAccountId = self.ctid_trader_account_id

        print(f"Sending ProtoOASymbolByIdReq: {req}")
        try:
            d = self._client.send(req)
            # The callback will handle ProtoOASymbolByIdRes
            d.addCallbacks(self._handle_symbol_details_response, self._handle_send_error)
            print("Added callbacks to ProtoOASymbolByIdReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_get_symbol_details_request send command: {e}")
            self._last_error = f"Exception sending symbol details request: {e}"

    def _send_get_symbols_list_request(self) -> None:
        """Sends a ProtoOASymbolsListReq to get all symbols for the authenticated account."""
        if not self._ensure_valid_token(): # Should not be strictly necessary if called after successful auth, but good practice
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot get symbols list: Client not connected."
            print(self._last_error)
            return
        if not self.ctid_trader_account_id:
            self._last_error = "Cannot get symbols list: ctidTraderAccountId is not set."
            print(self._last_error)
            return

        print(f"Requesting symbols list for account {self.ctid_trader_account_id}")
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        # req.includeArchivedSymbols = False # Optional: to include archived symbols

        print(f"Sending ProtoOASymbolsListReq: {req}")
        try:
            d = self._client.send(req)
            d.addCallbacks(self._handle_symbols_list_response, self._handle_send_error)
            print("Added callbacks to ProtoOASymbolsListReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_get_symbols_list_request send command: {e}")
            self._last_error = f"Exception sending symbols list request: {e}"

    def _handle_symbols_list_response(self, response_wrapper: Any) -> None:
        """Handles the response from a ProtoOASymbolsListReq."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_symbols_list_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASymbolsListRes):
            print(f"_handle_symbols_list_response: Expected ProtoOASymbolsListRes, got {type(actual_message)}. Message: {actual_message}")
            self._last_error = "Symbols list response was not ProtoOASymbolsListRes."
            return

        print(f"Received ProtoOASymbolsListRes with {len(actual_message.symbol)} symbols.")
        self.symbols_map.clear()
        # self.symbol_details_map.clear() # Cleared by symbol_details_response if needed, or upon new full fetch
        self.default_symbol_id = None

        # The field in ProtoOASymbolsListRes is typically 'symbol' but contains ProtoOALightSymbol objects.
        # If the field name is different (e.g., 'lightSymbol'), this loop needs adjustment.
        # Assuming it's 'symbol' based on typical Protobuf generation.
        symbols_field = getattr(actual_message, 'symbol', []) # Default to empty list if field not found

        print(f"Received ProtoOASymbolsListRes with {len(symbols_field)} light symbols.")


        for light_symbol_proto in symbols_field: # These are ProtoOALightSymbol
            self.symbols_map[light_symbol_proto.symbolName] = light_symbol_proto.symbolId
            # print(f"  Light Symbol: {light_symbol_proto.symbolName}, ID: {light_symbol_proto.symbolId}")

            if light_symbol_proto.symbolName == self.settings.general.default_symbol:
                self.default_symbol_id = light_symbol_proto.symbolId
                print(f"Found default_symbol: '{self.settings.general.default_symbol}' with ID: {self.default_symbol_id} (Light symbol details). Requesting full details.")

        self.symbol_id_to_name_map = {v: k for k, v in self.symbols_map.items()}
        print(f"Built symbol ID to name map. Total symbols: {len(self.symbol_id_to_name_map)}")

        if self.default_symbol_id is not None:
            # Now that we have the ID, request full details for the default symbol
            self._send_get_symbol_details_request([self.default_symbol_id])
        else:
            print(f"Warning: Default symbol '{self.settings.general.default_symbol}' not found in symbols list for account {self.ctid_trader_account_id}.")
            self._last_error = f"Default symbol '{self.settings.general.default_symbol}' not found."

    def _handle_symbol_details_response(self, response_wrapper: Any) -> None:
        """Handles the response from a ProtoOASymbolByIdReq, containing full symbol details."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_symbol_details_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASymbolByIdRes):
            print(f"_handle_symbol_details_response: Expected ProtoOASymbolByIdRes, got {type(actual_message)}. Message: {actual_message}")
            self._last_error = "Symbol details response was not ProtoOASymbolByIdRes."
            # Potentially try to re-request or handle error for specific symbols if needed.
            return

        print(f"Received ProtoOASymbolByIdRes with details for {len(actual_message.symbol)} symbol(s).")

        for detailed_symbol_proto in actual_message.symbol: # These are full ProtoOASymbol objects
            symbol_id = detailed_symbol_proto.symbolId
            symbol_name = self.symbol_id_to_name_map.get(symbol_id)

            if not symbol_name:
                print(f"  Warning: Got details for symbol ID {symbol_id} but no name in map. Skipping.")
                continue

            self.symbol_details_map[symbol_id] = detailed_symbol_proto
            print(f"  Stored full details for Symbol ID: {symbol_id} ({symbol_name}), Digits: {detailed_symbol_proto.digits}, PipPosition: {detailed_symbol_proto.pipPosition}")

            # Now that we have details, check if we need to fetch historical data for this symbol.
            if self.ohlc_history.get(symbol_name, {}).get('1m', pd.DataFrame()).empty:
                print(f"Historical 1m data for '{symbol_name}' is missing. Requesting...")
                self._send_get_trendbars_request(
                    symbol_id=symbol_id,
                    period=ProtoOATrendbarPeriod.M1,
                    count=self.max_ohlc_history_len
                )

            # After getting details, ensure we are subscribed to live spots for this symbol.
            if symbol_id not in self.subscribed_spot_symbol_ids:
                if self.ctid_trader_account_id:
                    print(f"Subscribing to spots for {symbol_name} (ID: {symbol_id}) after getting details.")
                    self._send_subscribe_spots_request(self.ctid_trader_account_id, [symbol_id])
                    self.subscribed_spot_symbol_ids.add(symbol_id)
                else:
                    print(f"Error: Cannot subscribe to spots for {symbol_name}, no ctidTraderAccountId.")
                    self._last_error = "ctidTraderAccountId not available for spot subscription."


    def _handle_account_auth_response(self, response: ProtoOAAccountAuthRes) -> None:
        print(f"Received ProtoOAAccountAuthRes: {response}")
        # The response contains the ctidTraderAccountId that was authenticated.
        # We should verify it matches the one we intended to authenticate.
        if response.ctidTraderAccountId == self.ctid_trader_account_id:
            print(f"Successfully authenticated account {self.ctid_trader_account_id}.")
            self.is_connected = True # Mark as connected for this specific account
            self._last_error = ""     # Clear any previous errors

            # After successful account auth, fetch initial trader details (balance, equity)
            self._send_get_trader_request(self.ctid_trader_account_id)

            # TODO: Subscribe to spots, etc., as needed by the application
            # self._send_subscribe_spots_request(symbol_id) # Example

            # After successful account auth, fetch initial state
            print("Account authenticated. Requesting initial state (symbols)...")
            self._send_get_symbols_list_request()
            self.start_equity_updater()

        else:
            print(f"AccountAuth failed. Expected ctidTraderAccountId {self.ctid_trader_account_id}, "
                  f"but response was for {response.ctidTraderAccountId if hasattr(response, 'ctidTraderAccountId') else 'unknown'}.")
            self._last_error = "Account authentication failed (ID mismatch or error)."
            self.is_connected = False
            # Consider stopping the client if account auth fails critically
            if self._client:
                self._client.stopService()

    def _handle_get_account_list_response(self, response: ProtoOAGetAccountListByAccessTokenRes) -> None:
        print("Account list response.")
        accounts = getattr(response, 'ctidTraderAccount', [])
        if not accounts:
            print("No accounts available for this access token.")
            self._last_error = "No trading accounts found for this access token."
            # Potentially disconnect or signal error more formally if no accounts mean connection cannot proceed.
            if self._client and self._is_client_connected:
                self._client.stopService() # Or some other error state
            return

        # TODO: If multiple accounts, allow user selection. For now, using the first.
        selected_account = accounts[0] # Assuming ctidTraderAccount is a list of ProtoOACtidTraderAccount
        if not selected_account.ctidTraderAccountId:
            print("Error: Account in list has no ctidTraderAccountId.")
            self._last_error = "Account found but missing ID."
            return

        self.ctid_trader_account_id = selected_account.ctidTraderAccountId
        print(f"Selected ctidTraderAccountId from list: {self.ctid_trader_account_id}")
        # Optionally save to settings if this discovery should update the default
        # self.settings.openapi.default_ctid_trader_account_id = self.ctid_trader_account_id

        # Now that we have a ctidTraderAccountId, authenticate this account
        self._send_account_auth_request(self.ctid_trader_account_id)

    def _handle_trader_response(self, response_wrapper: Any) -> None:
        # If this is called directly by a Deferred, response_wrapper might be ProtoMessage
        # If called after global _on_message_received, it's already extracted.
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_trader_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper # Assume it's already the specific message type

        if not isinstance(actual_message, ProtoOATraderRes):
            print(f"_handle_trader_response: Expected ProtoOATraderRes, got {type(actual_message)}. Message: {actual_message}")
            return

        # Now actual_message is definitely ProtoOATraderRes
        trader_object = actual_message.trader # Access the nested ProtoOATrader object
        
        trader_details_updated = self._update_trader_details(
            "Trader details response.", trader_object
        )

        if trader_details_updated and hasattr(trader_object, 'ctidTraderAccountId'):
            current_ctid = getattr(trader_object, 'ctidTraderAccountId')
            print(f"Value of trader_object.ctidTraderAccountId before assignment: {current_ctid}, type: {type(current_ctid)}")
            self.account_id = str(current_ctid)
            print(f"self.account_id set to: {self.account_id}")
        elif trader_details_updated:
            print(f"Trader details updated, but ctidTraderAccountId missing from trader_object. trader_object: {trader_object}")
        else:
            print("_handle_trader_response: _update_trader_details did not return updated details or trader_object was None.")


    def _handle_trader_updated_event(self, event_wrapper: Any) -> None:
        if isinstance(event_wrapper, ProtoMessage):
            actual_event = Protobuf.extract(event_wrapper)
            print(f"_handle_trader_updated_event: Extracted {type(actual_event)} from ProtoMessage wrapper.")
        else:
            actual_event = event_wrapper

        if not isinstance(actual_event, ProtoOATraderUpdatedEvent):
            print(f"_handle_trader_updated_event: Expected ProtoOATraderUpdatedEvent, got {type(actual_event)}. Message: {actual_event}")
            return
            
        self._update_trader_details(
            "Trader updated event.", actual_event.trader # Access nested ProtoOATrader
        )
        # Note: TraderUpdatedEvent might not always update self.account_id if it's already set,
        # but it will refresh balance, equity, margin if present in actual_event.trader.

    def _update_trader_details(self, log_message: str, trader_proto: ProtoOATrader):
        """Helper to update trader balance and equity from a ProtoOATrader object."""
        print(log_message)
        if trader_proto:
            print(f"DEBUG: Raw trader_proto object received:\n{trader_proto}\n")

            # Safely get ctidTraderAccountId for logging, though it's not set here directly
            logged_ctid = getattr(trader_proto, 'ctidTraderAccountId', 'N/A')

            balance_val = getattr(trader_proto, 'balance', None)
            if balance_val is not None:
                self.balance = balance_val / 100.0
                print(f"  Updated balance for {logged_ctid}: {self.balance}")
            else:
                print(f"  Balance not found in ProtoOATrader for {logged_ctid}")

            equity_val = getattr(trader_proto, 'equity', None)
            if equity_val is not None:
                self.equity = equity_val / 100.0
                print(f"  Updated equity for {logged_ctid}: {self.equity}")
            else:
                # self.equity remains as its previous value (or None if first time)
                print(f"  Equity not found in ProtoOATrader for {logged_ctid}. self.equity remains: {self.equity}")
            
            currency_val = getattr(trader_proto, 'depositAssetId', None) # depositAssetId is often used for currency ID
            # TODO: Convert depositAssetId to currency string if mapping is available
            # For now, just store the ID if it exists, or keep self.currency as is.
            if currency_val is not None:
                 # self.currency = str(currency_val) # Or map to symbol
                 print(f"  depositAssetId (currency ID) for {logged_ctid}: {currency_val}")


            used_margin_val = getattr(trader_proto, 'usedMargin', None)
            if used_margin_val is not None:
                self.used_margin = used_margin_val / 100.0

            free_margin_val = getattr(trader_proto, 'freeMargin', None)
            if free_margin_val is not None:
                self.free_margin = free_margin_val / 100.0

            margin_level_val = getattr(trader_proto, 'marginLevel', None)
            if margin_level_val is not None:
                self.margin_level = margin_level_val

            # If a callback is registered, invoke it with the latest summary
            if self.on_account_update:
                summary = self.get_account_summary()
                self.on_account_update(summary)

            return trader_proto
        else:
            print("_update_trader_details received empty trader_proto.")
        return None

    def _initialize_data_for_symbol(self, symbol_name: str):
        """Initializes the data structures for a new symbol if they don't exist."""
        if symbol_name not in self.price_histories:
            self.price_histories[symbol_name] = deque(maxlen=self.history_size)
        if symbol_name not in self.ohlc_history:
            self.ohlc_history[symbol_name] = {}
            self.current_bars[symbol_name] = {}
            for tf_str in self.timeframes_seconds.keys():
                self.ohlc_history[symbol_name][tf_str] = pd.DataFrame(
                    columns=['open', 'high', 'low', 'close', 'volume']
                )
                self.ohlc_history[symbol_name][tf_str].index.name = 'timestamp'
                self.current_bars[symbol_name][tf_str] = {
                    'timestamp': None, 'open': None, 'high': None, 'low': None, 'close': None, 'volume': 0
                }

    def _handle_spot_event(self, event: ProtoOASpotEvent) -> None:
        """Handles incoming spot events (price updates) for all subscribed symbols."""
        symbol_id = event.symbolId
        symbol_name = self.symbol_id_to_name_map.get(symbol_id)

        if not symbol_name:
            # Can happen for symbols not in our initial list.
            return

        # Initialize data structures for the symbol if this is the first tick
        self._initialize_data_for_symbol(symbol_name)

        # Timestamp handling
        if event.timestamp == 0:
            event_dt = datetime.now(timezone.utc)
            # print(f"WARNING: SpotEvent for {symbol_name} has no timestamp; falling back to local time {event_dt.isoformat()}")
        else:
            event_dt = datetime.fromtimestamp(event.timestamp / 1000, tz=timezone.utc)

        # Price scaling
        raw_bid = event.bid
        price_scale = 100000.0
        if symbol_id in self.symbol_details_map:
            price_scale = float(10 ** self.symbol_details_map[symbol_id].digits)
        current_price = raw_bid / price_scale

        # Update latest price and history
        self.latest_prices[symbol_name] = current_price
        self.price_histories[symbol_name].append(current_price)

        # OHLC Aggregation Logic
        for tf_str, tf_seconds in self.timeframes_seconds.items():
            current_tf_bar = self.current_bars[symbol_name][tf_str]

            if current_tf_bar['timestamp'] is None: # First tick for this symbol/timeframe
                bar_start_time = event_dt.replace(second=(event_dt.second // tf_seconds) * tf_seconds, microsecond=0)
                current_tf_bar.update({
                    'timestamp': bar_start_time, 'open': current_price, 'high': current_price,
                    'low': current_price, 'close': current_price, 'volume': 1
                })
            else:
                bar_end_time = current_tf_bar['timestamp'] + pd.Timedelta(seconds=tf_seconds)
                if event_dt >= bar_end_time:
                    # Finalize the completed bar
                    completed_bar_df = pd.DataFrame([{
                        'open': current_tf_bar['open'], 'high': current_tf_bar['high'],
                        'low': current_tf_bar['low'], 'close': current_tf_bar['close'],
                        'volume': current_tf_bar['volume']
                    }], index=[current_tf_bar['timestamp']])
                    completed_bar_df.index.name = 'timestamp'

                    # Append to history, avoiding concat with empty dataframe
                    history_df = self.ohlc_history[symbol_name][tf_str]
                    if history_df.empty:
                        self.ohlc_history[symbol_name][tf_str] = completed_bar_df
                    else:
                        self.ohlc_history[symbol_name][tf_str] = pd.concat([
                            history_df, completed_bar_df
                        ])

                    # Trim history
                    history_df = self.ohlc_history[symbol_name][tf_str]
                    if len(history_df) > self.max_ohlc_history_len:
                        self.ohlc_history[symbol_name][tf_str] = history_df.iloc[-self.max_ohlc_history_len:]

                    # Start a new bar
                    bar_start_time = event_dt.replace(second=(event_dt.second // tf_seconds) * tf_seconds, microsecond=0)
                    current_tf_bar.update({
                        'timestamp': bar_start_time, 'open': current_price, 'high': current_price,
                        'low': current_price, 'close': current_price, 'volume': 1
                    })
                else:
                    # Update the currently forming bar
                    current_tf_bar['high'] = max(current_tf_bar['high'], current_price)
                    current_tf_bar['low'] = min(current_tf_bar['low'], current_price)
                    current_tf_bar['close'] = current_price
                    current_tf_bar['volume'] += 1

            # # Build OHLC bars for each timeframe
            # for tf_str, tf_seconds in self.timeframes_seconds.items():
            #     bar = self.current_bars[tf_str]

            #     # Initialize bar if empty
            #     if bar['timestamp'] is None:
            #         start_ts = event_dt.replace(second=(event_dt.second // tf_seconds) * tf_seconds, microsecond=0)
            #         bar.update({
            #             'timestamp': start_ts,
            #             'open': current_price,
            #             'high': current_price,
            #             'low': current_price,
            #             'close': current_price,
            #             'volume': 1
            #         })
            #     else:
            #         bar_end = bar['timestamp'] + pd.Timedelta(seconds=tf_seconds)
            #         if event_dt >= bar_end:
            #             # Finalize and store completed bar
            #             completed = {
            #                 'timestamp': bar['timestamp'],
            #                 'open': bar['open'],
            #                 'high': bar['high'],
            #                 'low': bar['low'],
            #                 'close': bar['close'],
            #                 'volume': bar['volume']
            #             }
            #             self.ohlc_history[tf_str] = pd.concat([
            #                 self.ohlc_history[tf_str],
            #                 pd.DataFrame([completed])
            #             ], ignore_index=True)
            #             # Trim history length
            #             if len(self.ohlc_history[tf_str]) > self.max_ohlc_history_len:
            #                 self.ohlc_history[tf_str] = self.ohlc_history[tf_str].iloc[-self.max_ohlc_history_len:]
            #             # Start new bar
            #             start_ts = event_dt.replace(second=(event_dt.second // tf_seconds) * tf_seconds, microsecond=0)
            #             bar.update({
            #                 'timestamp': start_ts,
            #                 'open': current_price,
            #                 'high': current_price,
            #                 'low': current_price,
            #                 'close': current_price,
            #                 'volume': 1
            #             })
            #         else:
            #             # Update ongoing bar
            #             bar['high'] = max(bar['high'], current_price)
            #             bar['low'] = min(bar['low'], current_price)
            #             bar['close'] = current_price
            #             bar['volume'] += 1

        
    def get_available_symbol_names(self) -> List[str]:
        """Returns a sorted list of symbol name strings available from the API."""
        if not self.symbols_map:
            return []
        return sorted(list(self.symbols_map.keys()))

    def _handle_execution_event(self, event: ProtoOAExecutionEvent) -> None:
        """Handles execution events to track open positions by focusing on the position object."""
        position_data = event.position

        # An execution event must have position data with a valid ID to be trackable.
        if not hasattr(position_data, 'positionId') or not position_data.positionId:
            return

        position_id = position_data.positionId
        status = position_data.positionStatus

        if status == ProtoOAPositionStatus.POSITION_STATUS_OPEN:
            symbol_name = self.symbol_id_to_name_map.get(position_data.symbolId)
            if not symbol_name:
                print(f"Warning: Execution event for unknown symbol ID {position_data.symbolId}")
                return

            symbol_details = self.symbol_details_map.get(position_data.symbolId)
            if not symbol_details or not symbol_details.lotSize:
                self._send_get_symbol_details_request([position_data.symbolId])
                print(f"Warning: Missing details for symbol {symbol_name}, cannot track position yet. Requesting details.")
                return

            volume_in_lots = position_data.tradeData.volume / symbol_details.lotSize

            new_pos = Position(
                position_id=position_id,
                symbol_name=symbol_name,
                trade_side=ProtoOATradeSide.Name(position_data.tradeData.tradeSide),
                volume_lots=volume_in_lots,
                open_price=position_data.price,
                open_timestamp=position_data.tradeData.openTimestamp
            )
            self.open_positions[position_id] = new_pos
            print(f"Position opened/updated: {new_pos}")
            self.handle_symbol_selection(symbol_name)

        elif status == ProtoOAPositionStatus.POSITION_STATUS_CLOSED:
            if position_id in self.open_positions:
                closed_pos = self.open_positions.pop(position_id)
                print(f"Position closed: {closed_pos}")

    def _handle_send_error(self, failure: Any) -> None:
        print(f"Send error: {failure.getErrorMessage()}")
        if hasattr(failure, 'printTraceback'):
            print("Traceback for send error:")
            failure.printTraceback(file=sys.stderr)
        else:
            print("Failure object does not have printTraceback method. Full failure object:")
            print(failure)
        self._last_error = failure.getErrorMessage()

    # Sending methods
    def _send_account_auth_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return # Token refresh failed or no token, error set by _ensure_valid_token

        print(f"Requesting AccountAuth for {ctid} with token: {self._access_token[:20]}...") # Log token used
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = ctid
        req.accessToken = self._access_token or "" # Should be valid now

        print(f"Sending ProtoOAAccountAuthReq for ctid {ctid}: {req}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for ProtoOAAccountAuthReq: {d}")

            def success_callback(response_msg):
                # This callback is mostly for confirming the Deferred fired successfully.
                # Normal processing will happen in _on_message_received if message is dispatched.
                print(f"AccountAuthReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")
                # Note: We don't directly process response_msg here as _on_message_received should get it.

            def error_callback(failure_reason):
                print(f"AccountAuthReq error_callback triggered. Failure:")
                # Print a summary of the failure, and the full traceback if it's an exception
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'printTraceback'):
                    print(f"  Traceback for AccountAuthReq error:")
                    failure_reason.printTraceback(file=sys.stderr)
                else:
                    print(f"  Failure object (no printTraceback): {failure_reason}")
                self._handle_send_error(failure_reason) # Ensure our existing error handler is called

            d.addCallbacks(success_callback, errback=error_callback)
            print("Added callbacks to AccountAuthReq Deferred.")

        except Exception as e:
            print(f"Exception during _send_account_auth_request send command: {e}")
            self._last_error = f"Exception sending AccountAuth: {e}"
            # Potentially stop client if send itself fails critically
            if self._client and self._is_client_connected:
                self._client.stopService()
                self.is_connected = False # Ensure state reflects this

    def _send_get_account_list_request(self) -> None:
        if not self._ensure_valid_token():
            return

        print("Requesting account list.")
        req = ProtoOAGetAccountListByAccessTokenReq()
        if not self._access_token: # Should have been caught by _ensure_valid_token, but double check for safety
            self._last_error = "Critical: OAuth access token not available for GetAccountList request."
            print(self._last_error)
            if self._client:
                self._client.stopService()
            return
        req.accessToken = self._access_token
        print(f"Sending ProtoOAGetAccountListByAccessTokenReq: {req}")
        d = self._client.send(req)
        d.addCallbacks(self._handle_get_account_list_response, self._handle_send_error)

    def _send_get_trader_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return

        print(f"Requesting Trader details for {ctid}")
        req = ProtoOATraderReq()
        req.ctidTraderAccountId = ctid
        # Note: ProtoOATraderReq does not directly take an access token in its fields.
        # The authentication is expected to be session-based after AccountAuth.
        # If a token were needed here, the message definition would include it.
        print(f"Sending ProtoOATraderReq for ctid {ctid}: {req}")
        d = self._client.send(req)
        d.addCallbacks(self._handle_trader_response, self._handle_send_error)

    def _send_get_ctid_profile_request(self) -> None:
        """Sends a ProtoOAGetCtidProfileByTokenReq using the current OAuth access token."""
        if not self._ensure_valid_token(): # Ensure token is valid before using it
            return

        if not self._access_token:
            self._last_error = "Critical: OAuth access token not available for GetCtidProfile request."
            print(self._last_error)
            if self._client and self._is_client_connected:
                self._client.stopService()
            return

        print("Sending ProtoOAGetCtidProfileByTokenReq...")
        req = ProtoOAGetCtidProfileByTokenReq()
        req.accessToken = self._access_token

        print(f"Sending ProtoOAGetCtidProfileByTokenReq: {req}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for ProtoOAGetCtidProfileByTokenReq: {d}")

            # Adding specific callbacks for this request to see its fate
            def profile_req_success_callback(response_msg):
                print(f"GetCtidProfileByTokenReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")

            def profile_req_error_callback(failure_reason):
                print(f"GetCtidProfileByTokenReq error_callback triggered. Failure:")
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'printTraceback'): # May be verbose
                    print(f"  Traceback for GetCtidProfileByTokenReq error:")
                    failure_reason.printTraceback(file=sys.stderr)
                else:
                    print(f"  Failure object (no printTraceback): {failure_reason}")
                self._handle_send_error(failure_reason)

            d.addCallbacks(profile_req_success_callback, errback=profile_req_error_callback)
            print("Added callbacks to GetCtidProfileByTokenReq Deferred.")

        except Exception as e:
            print(f"Exception during _send_get_ctid_profile_request send command: {e}")
            self._last_error = f"Exception sending GetCtidProfile: {e}"
            if self._client and self._is_client_connected:
                self._client.stopService()
                self.is_connected = False

    def _send_subscribe_spots_request(self, ctid_trader_account_id: int, symbol_ids: List[int]) -> None:
        """Sends a ProtoOASubscribeSpotsReq to subscribe to spot prices for given symbol IDs."""
        if not self._ensure_valid_token():
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot subscribe to spots: Client not connected."
            print(self._last_error)
            return
        if not ctid_trader_account_id:
            self._last_error = "Cannot subscribe to spots: ctidTraderAccountId is not set."
            print(self._last_error)
            return
        if not symbol_ids:
            self._last_error = "Cannot subscribe to spots: No symbol_ids provided."
            print(self._last_error)
            return

        print(f"Requesting spot subscription for account {ctid_trader_account_id} and symbols {symbol_ids}")
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = ctid_trader_account_id
        req.symbolId.extend(symbol_ids) # symbolId is a repeated field

        # clientMsgId can be set if needed, but for subscriptions, the server pushes updates
        # req.clientMsgId = self._next_message_id()

        print(f"Sending ProtoOASubscribeSpotsReq: {req}")
        try:
            d = self._client.send(req)
            # Add callbacks: one for the direct response to the subscription request,
            # and one for handling errors during sending.
            # Spot events themselves will be handled by _on_message_received -> _handle_spot_event.
            d.addCallbacks(
                lambda response: self._handle_subscribe_spots_response(response, symbol_ids),
                self._handle_send_error
            )
            print("Added callbacks to ProtoOASubscribeSpotsReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_subscribe_spots_request send command: {e}")
            self._last_error = f"Exception sending spot subscription: {e}"


    # Public API
    def connect(self) -> bool:
        """
        Establishes a connection to the trading service.
        Handles OAuth2 flow (token loading, refresh, or full browser authentication)
        and then starts the underlying OpenAPI client service.

        Returns:
            True if connection setup (including successful client service start) is successful,
            False otherwise.
        """
        if not USE_OPENAPI_LIB:
            print("Mock mode: OpenAPI library unavailable.")
            self._last_error = "OpenAPI library not available (mock mode)."
            return False

        # 1. Check if loaded token is valid and not expired
        self._last_error = "Checking saved tokens..." # For GUI
        if self._access_token and not self._is_token_expired():
            print("Using previously saved, valid access token.")
            self._last_error = "Attempting to connect with saved token..." # For GUI
            if self._start_openapi_client_service():
                return True # Proceed with this token
            else:
                # Problem starting client service even with a seemingly valid token
                # self._last_error is set by _start_openapi_client_service
                print(f"Failed to start client service with saved token: {self._last_error}")
                # Fall through to try refresh or full OAuth
                self._access_token = None # Invalidate to ensure we don't retry this path immediately

        # 2. If token is expired or (now) no valid token, try to refresh if possible
        if self._refresh_token: # Check if refresh is even possible
            if self._access_token and self._is_token_expired(): # If previous step invalidated or was originally expired
                print("Saved access token is (or became) invalid/expired, attempting refresh...")
            elif not self._access_token: # No access token from file, but refresh token exists
                 print("No saved access token, but refresh token found. Attempting refresh...")

            self._last_error = "Attempting to refresh token..." # For GUI
            if self.refresh_access_token(): # This also saves the new token
                print("Access token refreshed successfully using saved refresh token.")
                self._last_error = "Token refreshed. Attempting to connect..." # For GUI
                if self._start_openapi_client_service():
                    return True # Proceed with refreshed token
                else:
                    # self._last_error set by _start_openapi_client_service
                    print(f"Failed to start client service after token refresh: {self._last_error}")
                    return False # Explicitly fail here if client service fails after successful refresh
            else:
                print("Failed to refresh token. Proceeding to full OAuth flow.")
                # self._last_error set by refresh_access_token(), fall through to full OAuth
        else:
            print("No refresh token available. Proceeding to full OAuth if needed.")


        # 3. If no valid/refreshed token, proceed to full OAuth browser flow
        print("No valid saved token or refresh failed/unavailable. Initiating full OAuth2 flow...")
        self._last_error = "OAuth2: Redirecting to browser for authentication." # More specific initial status

        auth_url = self.settings.openapi.spotware_auth_url
        # token_url = self.settings.openapi.spotware_token_url # Not used in this part of connect()
        client_id = self.settings.openapi.client_id
        redirect_uri = self.settings.openapi.redirect_uri
        # Define scopes - this might need adjustment based on Spotware's requirements
        scopes = "trading" # Changed from "trading accounts" to just "accounts" based on OpenApiPy example hint

        # Construct the authorization URL using the new Spotware URL
        # Construct the authorization URL using the standard Spotware OAuth endpoint.
        params = {
            "response_type": "code", # Required for Authorization Code flow
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes
            # product="web" is removed as it's not part of standard OAuth params here
            # "state": "YOUR_UNIQUE_STATE_HERE" # Optional: for CSRF protection
        }
        auth_url_with_params = f"{auth_url}?{urllib.parse.urlencode(params)}"

        # Start local HTTP server
        if not self._start_local_http_server():
            self._last_error = "OAuth2 Error: Could not start local HTTP server for callback."
            print(self._last_error)
            return False # Indicate connection failed

        print(f"Redirecting to browser for authentication: {auth_url_with_params}")
        webbrowser.open(auth_url_with_params)
        self._last_error = "OAuth2: Waiting for authorization code via local callback..." # Update status

        # Wait for the auth code from the HTTP server (with a timeout)
        try:
            auth_code = self._auth_code_queue.get(timeout=120) # 2 minutes timeout
            print("Authorization code received from local server.")
        except queue.Empty:
            print("OAuth2 Error: Timeout waiting for authorization code from callback.")
            self._last_error = "OAuth2 Error: Timeout waiting for callback."
            self._stop_local_http_server()
            return False

        self._stop_local_http_server()

        if auth_code:
            return self.exchange_code_for_token(auth_code)
        else: # Should not happen if queue contained None or empty string but good to check
            self._last_error = "OAuth2 Error: Invalid authorization code received."
            print(self._last_error)
            return False


    def _start_local_http_server(self) -> bool:
        """
        Starts a local HTTP server on a separate thread to listen for the OAuth callback.
        The server address is determined by self.settings.openapi.redirect_uri.

        Returns:
            True if the server started successfully, False otherwise.
        """
        try:
            # Ensure any previous server is stopped
            if self._http_server_thread and self._http_server_thread.is_alive():
                self._stop_local_http_server()

            # Use localhost and port from redirect_uri
            parsed_uri = urllib.parse.urlparse(self.settings.openapi.redirect_uri)
            host = parsed_uri.hostname
            port = parsed_uri.port

            if not host or not port:
                print(f"Invalid redirect_uri for local server: {self.settings.openapi.redirect_uri}")
                return False

            # Pass the queue to the handler
            def handler_factory(*args, **kwargs):
                return OAuthCallbackHandler(*args, auth_code_queue=self._auth_code_queue, **kwargs)

            self._http_server = HTTPServer((host, port), handler_factory)
            self._http_server_thread = threading.Thread(target=self._http_server.serve_forever, daemon=True)
            self._http_server_thread.start()
            print(f"Local HTTP server started on {host}:{port} for OAuth callback.")
            return True
        except Exception as e:
            print(f"Failed to start local HTTP server: {e}")
            self._last_error = f"Failed to start local HTTP server: {e}"
            return False

    def _stop_local_http_server(self):
        if self._http_server:
            print("Shutting down local HTTP server...")
            self._http_server.shutdown() # Signal server to stop serve_forever loop
            self._http_server.server_close() # Close the server socket
            self._http_server = None
        if self._http_server_thread and self._http_server_thread.is_alive():
            self._http_server_thread.join(timeout=5) # Wait for thread to finish
            if self._http_server_thread.is_alive():
                print("Warning: HTTP server thread did not terminate cleanly.")
        self._http_server_thread = None
        print("Local HTTP server stopped.")


    def exchange_code_for_token(self, auth_code: str) -> bool:
       
        print(f"Exchanging authorization code for token: {auth_code[:20]}...") # Log part of the code
        self._last_error = ""
        try:
            token_url = self.settings.openapi.spotware_token_url
            payload = {
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": self.settings.openapi.redirect_uri,
                "client_id": self.settings.openapi.client_id,
                "client_secret": self.settings.openapi.client_secret,
            }
            response = requests.post(token_url, data=payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

            token_data = response.json()

            if "access_token" not in token_data:
                self._last_error = "OAuth2 Error: access_token not in response from token endpoint."
                print(f"{self._last_error} Response: {token_data}")
                return False

            self._access_token = token_data["access_token"]
            self._refresh_token = token_data.get("refresh_token") # refresh_token might not always be present
            expires_in = token_data.get("expires_in")
            if expires_in:
                self._token_expires_at = time.time() + int(expires_in)
            else:
                self._token_expires_at = None # Or a very long time if not specified

            print(f"Access token obtained: {self._access_token[:20]}...")
            if self._refresh_token:
                print(f"Refresh token obtained: {self._refresh_token[:20]}...")
            print(f"Token expires in: {expires_in} seconds (at {self._token_expires_at})")

            # Now that we have the access token, we can start the actual OpenAPI client service
            self._save_tokens_to_file() # Save tokens after successful exchange
            if self._start_openapi_client_service():
                # Connection to TCP endpoint will now proceed, leading to ProtoOAApplicationAuthReq etc.
                # The _check_connection in GUI will handle the rest.
                return True
            else:
                # _start_openapi_client_service would have set _last_error
                return False

        except requests.exceptions.HTTPError as http_err:
            error_content = http_err.response.text
            self._last_error = f"OAuth2 HTTP Error: {http_err}. Response: {error_content}"
            print(self._last_error)
            return False
        except requests.exceptions.RequestException as req_err:
            self._last_error = f"OAuth2 Request Error: {req_err}"
            print(self._last_error)
            return False
        except Exception as e:
            self._last_error = f"OAuth2 Unexpected Error during token exchange: {e}"
            print(self._last_error)
            return False

    def _start_openapi_client_service(self):
        """
        Starts the underlying OpenAPI client service (TCP connection, reactor).
        This is called after successful OAuth token acquisition/validation.

        Returns:
            True if the client service started successfully, False otherwise.
        """
        if self.is_connected or (self._client and getattr(self._client, 'isConnected', False)):
            print("OpenAPI client service already running or connected.")
            return True

        print("Starting OpenAPI client service.")
        try:
            self._client.startService()
            if _reactor_installed and not reactor.running:
                self._reactor_thread = threading.Thread(target=lambda: reactor.run(installSignalHandlers=0), daemon=True)
                self._reactor_thread.start()
            return True
        except Exception as e:
            print(f"Error starting OpenAPI client service: {e}")
            self._last_error = f"OpenAPI client error: {e}"
            return False

    def refresh_access_token(self) -> bool:
        """
        Refreshes the OAuth access token using the stored refresh token.
        Saves the new tokens to file on success.

        Returns:
            True if the access token was refreshed successfully, False otherwise.
        """
        if not self._refresh_token:
            self._last_error = "OAuth2 Error: No refresh token available to refresh access token."
            print(self._last_error)
            return False

        print("Attempting to refresh access token...")
        self._last_error = ""
        try:
            token_url = self.settings.openapi.spotware_token_url
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self.settings.openapi.client_id,
                "client_secret": self.settings.openapi.client_secret, # Typically required for refresh
            }
            response = requests.post(token_url, data=payload)
            response.raise_for_status()

            token_data = response.json()

            if "access_token" not in token_data:
                self._last_error = "OAuth2 Error: access_token not in response from refresh token endpoint."
                print(f"{self._last_error} Response: {token_data}")
                # Potentially invalidate old tokens if refresh fails this way
                self.is_connected = False
                return False

            self._access_token = token_data["access_token"]
            # A new refresh token might be issued, or the old one might continue to be valid.
            # Standard practice: if a new one is issued, use it. Otherwise, keep the old one.
            if "refresh_token" in token_data:
                self._refresh_token = token_data["refresh_token"]
                print(f"New refresh token obtained: {self._refresh_token[:20]}...")

            expires_in = token_data.get("expires_in")
            if expires_in:
                self._token_expires_at = time.time() + int(expires_in)
            else:
                # If expires_in is not provided on refresh, it might mean the expiry doesn't change
                # or it's a non-expiring token (less common). For safety, clear old expiry.
                self._token_expires_at = None

            print(f"Access token refreshed successfully: {self._access_token[:20]}...")
            print(f"New expiry: {self._token_expires_at}")
            self._save_tokens_to_file() # Save tokens after successful refresh
            return True

        except requests.exceptions.HTTPError as http_err:
            error_content = http_err.response.text
            self._last_error = f"OAuth2 HTTP Error during token refresh: {http_err}. Response: {error_content}"
            print(self._last_error)
            self.is_connected = False # Assume connection is lost if refresh fails
            return False
        except requests.exceptions.RequestException as req_err:
            self._last_error = f"OAuth2 Request Error during token refresh: {req_err}"
            print(self._last_error)
            self.is_connected = False
            return False
        except Exception as e:
            self._last_error = f"OAuth2 Unexpected Error during token refresh: {e}"
            print(self._last_error)
            self.is_connected = False
            return False

    def _is_token_expired(self, buffer_seconds: int = 60) -> bool:
        """
        Checks if the current OAuth access token is expired or nearing expiry.

        Args:
            buffer_seconds: A buffer time in seconds. If the token expires within
                            this buffer, it's considered nearing expiry.

        Returns:
            True if the token is non-existent, expired, or nearing expiry, False otherwise.
        """
        if not self._access_token:
            return True # No token means it's effectively expired for use
        if self._token_expires_at is None:
            return False # Token that doesn't expire (or expiry unknown)
        return time.time() > (self._token_expires_at - buffer_seconds)

    def _ensure_valid_token(self) -> bool:
       
        if self._is_token_expired():
            print("Access token expired or nearing expiry. Attempting refresh.")
            if not self.refresh_access_token():
                print("Failed to refresh access token.")
                # self._last_error is set by refresh_access_token()
                if self._client and self._is_client_connected: # Check if client exists and was connected
                    self._client.stopService() # Stop service if token cannot be refreshed
                self.is_connected = False
                return False
        return True


    def disconnect(self) -> None:
        self.stop_equity_updater()
        if self._client:
            self._client.stopService()
        if _reactor_installed and reactor.running:
            reactor.callFromThread(reactor.stop)
        self.is_connected = False
        self._is_client_connected = False

    def get_connection_status(self) -> Tuple[bool, str]:
        return self.is_connected, self._last_error

    def request_account_update(self) -> None:
        """Sends a request to the server to get the latest trader account details."""
        if not self.is_connected or not self.ctid_trader_account_id:
            print("Cannot request account update: Not connected or no account ID.")
            return
        print(f"Requesting manual account update for {self.ctid_trader_account_id}...")
        self._send_get_trader_request(self.ctid_trader_account_id)

    def get_account_summary(self) -> Dict[str, Any]:
        if not USE_OPENAPI_LIB:
            return {"account_id": "MOCK", "balance": 0.0, "equity": 0.0, "margin": 0.0}

        return {
            "account_id": self.account_id if self.account_id else "connecting...",
            "balance": self.balance,
            "equity": self.equity,
            "used_margin": self.used_margin,
            "free_margin": self.free_margin,
            "margin_level": self.margin_level
        }

    def calculate_total_pnl(self) -> float:
        """
        Calculates the total Profit and Loss for all open positions.
        Also updates the `current_pnl` attribute for each position object.
        """
        total_pnl = 0.0
        for pos in self.open_positions.values():
            pos.current_pnl = 0.0 # Reset P&L before calculation
            current_price = self.get_market_price(pos.symbol_name)
            if current_price is None:
                continue

            symbol_id = self.symbols_map.get(pos.symbol_name)
            symbol_details = self.symbol_details_map.get(symbol_id)
            if not symbol_details:
                continue

            price_diff = current_price - pos.open_price
            if pos.trade_side == 'SELL':
                price_diff = -price_diff

            # This is P&L in quote currency.
            pnl_in_quote = price_diff * pos.volume_lots * symbol_details.lotSize

            # For now, we assume the quote currency is the account currency.
            # This is a simplifying assumption that works for pairs like EUR/USD on a USD account.
            # A more robust solution would handle cross-currency conversion.
            pos.current_pnl = pnl_in_quote
            total_pnl += pnl_in_quote
        return total_pnl

    def start_equity_updater(self):
        """Starts the background thread for updating equity."""
        if self._equity_update_thread is None or not self._equity_update_thread.is_alive():
            self._stop_equity_updater.clear()
            self._equity_update_thread = threading.Thread(target=self._equity_update_loop, daemon=True)
            self._equity_update_thread.start()
            print("Equity updater thread started.")

    def stop_equity_updater(self):
        """Stops the equity updater thread."""
        self._stop_equity_updater.set()
        if self._equity_update_thread:
            print("Stopping equity updater thread...")
            self._equity_update_thread.join(timeout=2)
            if self._equity_update_thread.is_alive():
                print("Warning: Equity updater thread did not terminate cleanly.")
            self._equity_update_thread = None

    def _equity_update_loop(self):
        """Periodically calculates P&L and updates equity and position data."""
        while not self._stop_equity_updater.is_set():
            if self.balance is not None and self.open_positions:
                pnl = self.calculate_total_pnl() # This now updates P&L on each position object
                new_equity = self.balance + pnl

                # Only push an account update if equity has changed to avoid flooding the queue
                if new_equity != self.equity:
                    self.equity = new_equity
                    if self.on_account_update:
                        summary = self.get_account_summary()
                        self.on_account_update(summary)

                # Always push position updates to ensure real-time P&L display
                if self.on_positions_update:
                    self.on_positions_update(self.open_positions)

            time.sleep(1) # Update interval

    def get_market_price(self, symbol: str) -> Optional[float]:
        if not USE_OPENAPI_LIB:
            # Mock mode: return a random price
            print(f"Mock mode: Returning random price for {symbol}")
            return round(random.uniform(1.10, 1.20), 5)
        return self.latest_prices.get(symbol)

    def get_price_history(self, symbol: str) -> List[float]:
        """Returns the price history for a specific symbol."""
        return list(self.price_histories.get(symbol, []))

    def get_ohlc_bar_counts(self, symbol_name: str) -> Dict[str, int]:
        """Returns a dictionary with the count of available OHLC bars for a specific symbol."""
        counts = {}
        if symbol_name and symbol_name in self.ohlc_history:
            for tf_str, df_history in self.ohlc_history[symbol_name].items():
                counts[tf_str] = len(df_history)
        return counts

    def handle_symbol_selection(self, symbol_name: str):
        """
        Handles the logic required when a user selects a new symbol in the GUI.
        Ensures that we have details, historical data, and a live price subscription.
        """
        symbol_id = self.symbols_map.get(symbol_name)
        if not symbol_id:
            print(f"Error: Cannot handle selection. Symbol '{symbol_name}' not found in map.")
            return

        # When a symbol is selected, we only need to ensure its details are loaded.
        # The response handler (_handle_symbol_details_response) will then trigger
        # historical data download and spot subscription, ensuring the correct order of operations.
        if symbol_id not in self.symbol_details_map:
            print(f"Details for '{symbol_name}' not found locally. Requesting...")
            self._send_get_symbol_details_request([symbol_id])
        else:
            # If we already have details, it implies history and subscriptions were already handled.
            # We can add a log for clarity.
            print(f"Details for '{symbol_name}' already loaded. No new requests needed.")

    def close_all_positions(self) -> None:
        """Closes all currently tracked open positions."""
        if not self.open_positions:
            print("No open positions to close.")
            return

        print(f"Requesting to close all {len(self.open_positions)} open positions...")
        # Create a copy of the keys to avoid issues with modifying the dict while iterating
        position_ids_to_close = list(self.open_positions.keys())
        for pos_id in position_ids_to_close:
            self.close_position(pos_id)

    def place_market_order(
        self,
        symbol_name: str,
        volume_lots: float,
        side: str,
        take_profit_pips: Optional[float] = None,
        stop_loss_pips: Optional[float] = None,
        client_msg_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Places a market order with a specified volume in lots.
        This method is now aligned with the GUI's direct volume input.
        """
        if not self.is_connected or not self._client or not self._is_client_connected:
            return False, "Not connected to the trading platform."

        if not self.ctid_trader_account_id:
            return False, "Account information not available for trading."

        symbol_id = self.symbols_map.get(symbol_name)
        if not symbol_id:
            return False, f"Symbol '{symbol_name}' not found."

        symbol_details = self.symbol_details_map.get(symbol_id)
        if not symbol_details:
            return False, f"Symbol details for '{symbol_name}' not loaded."

        # Convert volume in lots to volume in units for the API request
        volume_in_units = int(volume_lots * symbol_details.lotSize)

        # Validate and adjust volume against symbol's min/max/step constraints
        min_volume = symbol_details.minVolume
        max_volume = symbol_details.maxVolume
        step_volume = symbol_details.stepVolume

        if volume_in_units < min_volume:
            print(f"Warning: Requested volume {volume_in_units} is below minimum {min_volume}. Adjusting to minimum.")
            volume_in_units = min_volume
        elif volume_in_units > max_volume:
            print(f"Warning: Requested volume {volume_in_units} is above maximum {max_volume}. Adjusting to maximum.")
            volume_in_units = max_volume

        # Adjust volume to the nearest valid step
        if step_volume > 0:
            volume_in_units = int(round(volume_in_units / step_volume) * step_volume)

        # Build the order request
        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        req.symbolId = symbol_id
        req.orderType = ProtoOAOrderType.MARKET
        req.tradeSide = ProtoOATradeSide.BUY if side.upper() == "BUY" else ProtoOATradeSide.SELL
        req.volume = volume_in_units
        req.comment = f"Market order via GUI: {volume_lots} lots"

        if stop_loss_pips is not None and stop_loss_pips > 0:
            # Convert pips to the integer format required by the API
            req.relativeStopLoss = int(stop_loss_pips * (10 ** symbol_details.pipPosition))

        if take_profit_pips is not None and take_profit_pips > 0:
            # Convert pips to the integer format required by the API
            req.relativeTakeProfit = int(take_profit_pips * (10 ** symbol_details.pipPosition))

        if client_msg_id:
            req.clientOrderId = client_msg_id

        print(f"DEBUG: Sending ProtoOANewOrderReq:\n{req}")
        try:
            deferred = self._client.send(req)
            # Add callbacks for logging the result of the send operation
            deferred.addCallbacks(
                lambda response: print(f"DEBUG: Order request successful. Full Server Response:\n{response}"),
                lambda failure: print(f"DEBUG: Failed to send order request. Full Failure object:\n{failure}")
            )
            return True, f"Order request for {volume_in_units} units ({volume_lots} lots) of {symbol_name} sent."
        except Exception as e:
            traceback.print_exc() # Print full traceback for debugging
            return False, f"An exception occurred while placing the order: {e}"

    def get_ai_advice(self, symbol_name: str, intent: str, features: dict, bot_proposal: dict) -> Optional[AiAdvice]:
        """
        Sends data to the AI advisor and returns its recommendation.
        """
        if not self.settings.ai.use_ai_overseer or not self.settings.ai.advisor_url:
            print("AI Overseer is disabled or URL is not configured.")
            return None

        if not symbol_name:
            print("Cannot get AI advice: symbol_name not provided.")
            return None

        payload = {
            "pair": symbol_name.replace("/", ""),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "timeframe": "M1", # Hardcoded as per C# example
            "intent": intent,
            "features": features,
            "bot_proposal": bot_proposal
        }

        headers = {
            "Content-Type": "application/json"
        }
        if self.settings.ai.advisor_auth_token:
            headers["X-Advisor-Token"] = self.settings.ai.advisor_auth_token

        timeout_seconds = self.settings.ai.advisor_timeout_ms / 1000.0

        try:
            print(f"Sending payload to AI Advisor: {json.dumps(payload, indent=2)}")
            response = requests.post(
                self.settings.ai.advisor_url,
                json=payload,
                headers=headers,
                timeout=timeout_seconds
            )
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            data = response.json()
            print(f"Received response from AI Advisor: {data}")

            # Normalize response keys (similar to C# example)
            action = data.get("action") or data.get("direction")
            if isinstance(action, str):
                action = action.lower()
                if action == "buy": action = "long"
                if action == "sell": action = "short"

            confidence = data.get("confidence")
            if confidence is None:
                confidence_pct = data.get("confidence_pct")
                if confidence_pct is not None:
                    confidence = float(confidence_pct) / 100.0

            if action not in ["long", "short", "skip", "hold"] or confidence is None:
                print(f"AI Advisor returned invalid advice: action='{action}', conf={confidence}")
                return None

            return AiAdvice(
                action=action,
                confidence=float(confidence),
                sl_pips=data.get("sl_pips"),
                tp_pips=data.get("tp_pips"),
                reason=data.get("reason", "No reason provided.")
            )

        except requests.exceptions.Timeout:
            print(f"AI Advisor request timed out after {timeout_seconds} seconds.")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error communicating with AI Advisor: {e}")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error parsing AI Advisor response: {e}")
            return None
            
    def _handle_execution_event(self, event: ProtoOAExecutionEvent) -> None:
        """Handles execution events to track open positions by focusing on the position object."""
        position_data = event.position

        # An execution event must have position data with a valid ID to be trackable.
        if not hasattr(position_data, 'positionId') or not position_data.positionId:
            return

        position_id = position_data.positionId
        status = position_data.positionStatus

        if status == ProtoOAPositionStatus.POSITION_STATUS_OPEN:
            symbol_id = position_data.tradeData.symbolId
            symbol_name = self.symbol_id_to_name_map.get(symbol_id)
            if not symbol_name:
                print(f"Warning: Execution event for unknown symbol ID {symbol_id}")
                return

            symbol_details = self.symbol_details_map.get(symbol_id)
            if not symbol_details or not hasattr(symbol_details, 'lotSize') or not hasattr(symbol_details, 'digits'):
                self._send_get_symbol_details_request([symbol_id])
                print(f"Warning: Missing details for symbol {symbol_name}, cannot track position yet. Requesting details.")
                return

            volume_in_lots = position_data.tradeData.volume / symbol_details.lotSize

            # The 'price' field from ProtoOAPosition is already a correctly scaled double.
            open_price = position_data.price

            new_pos = Position(
                position_id=position_id,
                symbol_name=symbol_name,
                trade_side=ProtoOATradeSide.Name(position_data.tradeData.tradeSide),
                volume_lots=volume_in_lots,
                open_price=open_price,
                open_timestamp=position_data.tradeData.openTimestamp
            )
            self.open_positions[position_id] = new_pos
            print(f"Position opened/updated: {new_pos}")
            self.handle_symbol_selection(symbol_name)

        elif status == ProtoOAPositionStatus.POSITION_STATUS_CLOSED:
            if position_id in self.open_positions:
                closed_pos = self.open_positions.pop(position_id)
                print(f"Position closed: {closed_pos}")

    def _send_get_trendbars_request(
        self,
        symbol_id: int,
        period: ProtoOATrendbarPeriod,
        count: int
    ) -> None:
        if not self._ensure_valid_token():
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot get trendbars: Client not connected."
            print(self._last_error)
            return
        if not self.ctid_trader_account_id:
            self._last_error = "Cannot get trendbars: ctidTraderAccountId is not set."
            print(self._last_error)
            return

        print(f"Requesting {count} trendbars for symbol ID {symbol_id}, period {ProtoOATrendbarPeriod.Name(period)}")

        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        req.symbolId = symbol_id
        req.period = period

        # Compute timestamps
        to_timestamp = int(time.time() * 1000)

        period_seconds = TREND_BAR_PERIOD_SECONDS.get(period)
        if period_seconds is None:
            self._last_error = f"Unsupported trendbar period: {period}"
            print(self._last_error)
            return

        from_timestamp = to_timestamp - (count * period_seconds * 1000)

        req.fromTimestamp = from_timestamp
        req.toTimestamp = to_timestamp
        req.count = count  # Optional, but safe to include

        print(
            f"Sending ProtoOAGetTrendbarsReq:\n"
            f"  fromTimestamp: {from_timestamp}\n"
            f"  toTimestamp:   {to_timestamp}\n"
            f"  period:        {ProtoOATrendbarPeriod.Name(period)}\n"
            f"  symbolId:      {symbol_id}\n"
            f"  ctidTraderAccountId: {self.ctid_trader_account_id}"
        )

        try:
            d = self._client.send(req)
            d.addErrback(self._handle_send_error)
            print("Added errback to ProtoOAGetTrendbarsReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_get_trendbars_request send command: {e}")
            self._last_error = f"Exception sending trendbars request: {e}"

    def close_position(self, position_id: int):
        """Sends a request to close a specific position."""
        if not self.is_connected or not self.ctid_trader_account_id:
            print("Cannot close position: Not connected.")
            return

        position_to_close = self.open_positions.get(position_id)
        if not position_to_close:
            print(f"Cannot close position: Position ID {position_id} not found in tracked list.")
            return

        symbol_id = self.symbols_map.get(position_to_close.symbol_name)
        symbol_details = self.symbol_details_map.get(symbol_id)
        if not symbol_details:
            print(f"Cannot close position: Missing symbol details for {position_to_close.symbol_name}")
            return

        volume_in_units = int(position_to_close.volume_lots * symbol_details.lotSize)

        req = ProtoOAClosePositionReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        req.positionId = position_id
        req.volume = volume_in_units

        print(f"Sending request to close position {position_id} with volume {volume_in_units}")
        try:
            self._client.send(req)
        except Exception as e:
            print(f"Exception while sending close position request: {e}")

    def _handle_get_trendbars_response(self, response: ProtoOAGetTrendbarsRes) -> None:
        print(f"Received ProtoOAGetTrendbarsRes for symbol ID {response.symbolId}, period {ProtoOATrendbarPeriod.Name(response.period)} with {len(response.trendbar)} bars.")

        symbol_id = response.symbolId
        symbol_name = self.symbol_id_to_name_map.get(symbol_id)
        if not symbol_name:
            print(f"Warning: Received trendbars for unknown symbol ID {symbol_id}. Cannot process.")
            return

        period_enum = response.period
        tf_str_map = { ProtoOATrendbarPeriod.M1: '1m', ProtoOATrendbarPeriod.M5: '5m' }
        tf_str = tf_str_map.get(period_enum)
        if not tf_str:
            print(f"Warning: Received trendbars for unmapped period {ProtoOATrendbarPeriod.Name(period_enum)}. Cannot process.")
            return

        symbol_details = self.symbol_details_map.get(symbol_id)
        if not symbol_details or not hasattr(symbol_details, 'digits'):
            print(f"Warning: Symbol details not found for symbol ID {symbol_id}. Cannot process trendbar prices.")
            return
        price_scale_factor = 10**symbol_details.digits

        processed_bars = []
        for bar_data in response.trendbar:
            ts_millis = bar_data.utcTimestampInMinutes * 60 * 1000
            dt_object = datetime.fromtimestamp(ts_millis / 1000, tz=timezone.utc)
            low_price = bar_data.low / price_scale_factor
            open_price = (bar_data.low + bar_data.deltaOpen) / price_scale_factor
            high_price = (bar_data.low + bar_data.deltaHigh) / price_scale_factor
            close_price = (bar_data.low + bar_data.deltaClose) / price_scale_factor
            processed_bars.append({
                'timestamp': dt_object, 'open': open_price, 'high': high_price,
                'low': low_price, 'close': close_price, 'volume': bar_data.volume
            })

        if not processed_bars:
            print(f"No bars processed from ProtoOAGetTrendbarsRes for {symbol_name}/{tf_str}.")
            return

        processed_bars.sort(key=lambda x: x['timestamp'])
        new_df = pd.DataFrame(processed_bars).set_index('timestamp')

        self._initialize_data_for_symbol(symbol_name)
        self.ohlc_history[symbol_name][tf_str] = new_df
        print(f"Populated {tf_str} OHLC history for {symbol_name} with {len(new_df)} bars.")

        if not new_df.empty:
            self.current_bars[symbol_name][tf_str] = {
                'timestamp': None, 'open': None, 'high': None, 'low': None, 'close': None, 'volume': 0
            }
            print(f"Reset current_bar for {symbol_name}/{tf_str} to allow live aggregation post-history fetch.")

        # Potentially, trigger a GUI update for data readiness explicitly here if needed,
        # though the periodic GUI poll should pick it up.