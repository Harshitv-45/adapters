# ============================================================================
# Imports
# ============================================================================

import json
import logging
import threading
import time
import queue
from collections import deque
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from Motilal.motilal_mapper import MotilalMapper
from Motilal.api.order import MotilalOswalOrderAPI
from Motilal.api.portfolio import MotilalPortfolioAPI
from Motilal.api.auth import MotilalAuthAPI
from Motilal.motilal_websocket import MotilalWebSocket
from common.logging_setup import setup_entity_logging
from common.message_formatter import MessageFormatter
from common.redis_client import RedisClient

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ============================================================================
# MotilalAdapter Class
# ============================================================================

class MotilalAdapter:
    def __init__(self, entity_id=None, creds=None):
        self.entity_id = entity_id
        self.creds = creds or {}

        self.logger = setup_entity_logging(entity_id, broker="MOFL")
        mofl_creds_raw = self._extract_mofl_credentials(creds)
        
        if isinstance(mofl_creds_raw, str):
            try:
                mofl_creds = json.loads(mofl_creds_raw)
                
            except (json.JSONDecodeError, TypeError) as e:
                
                mofl_creds = mofl_creds_raw
        else:
            mofl_creds = mofl_creds_raw
            
        
        if isinstance(mofl_creds, list):
            self.logger.info(f"[CREDENTIAL EXTRACTION] Processing Name/Value array format with {len(mofl_creds)} items")
            self.api_key = self.get_cred_value(mofl_creds, {"apiKey", "api_key", "API_KEY"}) or getattr(config, 'MOTILAL_OSWAL_API_KEY', None) or ""
            self.api_secret = self.get_cred_value(mofl_creds, {"apiSecret", "api_secret", "API_SECRET"}) or getattr(config, 'MOTILAL_OSWAL_API_SECRET', None) or ""
            self.client_code = self.get_cred_value(mofl_creds, {"clientId", "ClientId", "CLIENT_ID", "client_id", "client_code", "CLIENT_CODE", "user_id", "USER_ID"}) or ""
            self.user_id = self.get_cred_value(mofl_creds, {"user_id", "USER_ID"}) or ""
            self.jwt_token = None
            self.access_token = None
            self.logger.info(f"[CREDENTIAL EXTRACTION] Extracted - api_key: {'***' if self.api_key else 'MISSING'}, client_code: {self.client_code or 'MISSING'}, api_secret: {'***' if self.api_secret else 'MISSING'}")
        else:
            self.api_key = mofl_creds.get("api_key") or mofl_creds.get("API_KEY") or getattr(config, 'MOTILAL_OSWAL_API_KEY', None) or ""
            self.api_secret = mofl_creds.get("api_secret") or mofl_creds.get("API_SECRET") or getattr(config, 'MOTILAL_OSWAL_API_SECRET', None) or ""
            self.client_code = (mofl_creds.get("CLIENT_ID") or mofl_creds.get("client_id") or 
                              mofl_creds.get("client_code") or mofl_creds.get("CLIENT_CODE") or 
                              mofl_creds.get("user_id") or mofl_creds.get("USER_ID") or "")
            self.user_id = mofl_creds.get("user_id") or mofl_creds.get("USER_ID") or ""
            self.jwt_token = None
            self.access_token = None


        self.redis_client = RedisClient()
        self.response_channel = config.CH_BLITZ_RESPONSES
        self.request_channel = config.CH_BLITZ_REQUESTS
        
        # Initialize MessageFormatter
        self.formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
        
        self.auth_api = MotilalAuthAPI(api_key=self.api_key, client_code=self.client_code) if self.api_key else None

        # Initialize login error tracking
        self.login_error_message = None
        self.login_error_code = None

        self.logger.info(f"Initializing MotilalAdapter for entity '{entity_id}'")
        self.logger.info("Attempting login via auth API to get token...")
        
        # Publish CONNECTING status when login starts
        self._publish_to_blitz_channel("LOGIN", "CONNECTING", "Connecting and logging in...")
        
        if isinstance(mofl_creds, list):
            password = self.get_cred_value(mofl_creds, {"password", "PASSWORD"})
            dob = self.get_cred_value(mofl_creds, {"dob", "DOB", "2FA"})
            login_creds = {
                "PASSWORD": password,
                "DOB": dob,
                "password": password,
                "dob": dob,
                "2FA": dob
            }
            self.logger.info(f"[CREDENTIAL EXTRACTION] Login credentials - password: {'***' if password else 'MISSING'}, dob: {dob or 'MISSING'}")
        else:
            login_creds = mofl_creds
            self.logger.info(f"[CREDENTIAL EXTRACTION] Using dictionary format for login")
        
        if self.auth_api and login_creds:
            auth_token = self._perform_login(login_creds)
            if auth_token:
                self.logger.info("Login successful! Token received from auth API.")
                self._publish_login_success()
            else:
                self.logger.warning("Login failed. Adapter may not work properly.")
                # Publish login failed message with stored error details
                error_msg = self.login_error_message or "Login failed"
                error_code = self.login_error_code or ""
                self._publish_login_failed(error_msg, error_code)
        else:
            self.logger.warning("Cannot perform login: auth_api or login credentials missing")
            self._publish_login_failed("Cannot perform login: auth_api or login credentials missing", "")

        if self.access_token or self.jwt_token:
            self.order_api = MotilalOswalOrderAPI(
                api_key=self.api_key,
                client_code=self.client_code,
                jwt_token=self.jwt_token or self.access_token,
                api_secret_key=self.api_secret,
                access_token=self.access_token or self.jwt_token
            )
            self.portfolio_api = MotilalPortfolioAPI(
                access_token=self.access_token or self.jwt_token,
                api_key=self.api_key,
                client_code=self.client_code
            )
            self.logger.info("APIs initialized with credentials. Ready to connect.")
            
            # Symbol cache will be loaded after websocket connects
        else:
            self.order_api = None
            self.portfolio_api = None
            self.logger.info("No access_token/jwt_token available. Adapter not ready.")

        self.logger.info(f"Motilal Adapter initialized successfully for entity '{entity_id}'.")


        self.blitz_to_motilal = {}
        self.motilal_to_blitz = {}
        self.order_cache = {}
        self.pubsub = None
        self.is_running = False
        self.websocket = None
        
        # Rate limiting: 10 requests per second
        self.rate_limit = 10  # requests per second
        self.rate_limit_window = 1.0  # 1 second window
        self.request_timestamps = deque()  # Track request timestamps
        self.request_queue = queue.Queue()  # Queue for requests that exceed rate limit
        self.rate_limit_lock = threading.Lock()  # Lock for thread-safe operations
        self.queue_processor_thread = None
        self._start_queue_processor()
    
    # ============================================================================
    # Credential Management
    # ============================================================================
    
    def _extract_mofl_credentials(self, creds):
        if not creds:
            return {}
        
        if isinstance(creds, dict) and "MOFL" in creds:
            return creds["MOFL"]
        
        if isinstance(creds, dict) and "Motilal" in creds:
            return creds["Motilal"]
        
        return creds
    
    def get_cred_value(self, creds, keys):
        for item in creds:
            name = item.get("Name")
            if name in keys:
                return item.get("Value")
        return ""
    
    # ============================================================================
    # Authentication
    # ============================================================================
    
    def _perform_login(self, mofl_creds):
        try:
            if not self.auth_api:
                if not self.api_key:
                    self.logger.error("Cannot create auth API: api_key is missing")
                    self.login_error_message = "Cannot create auth API: api_key is missing"
                    self.login_error_code = ""
                    return None
                self.auth_api = MotilalAuthAPI(api_key=self.api_key, client_code=self.client_code)
            
            password = mofl_creds.get("PASSWORD") or mofl_creds.get("password")
            dob = mofl_creds.get("DOB") or mofl_creds.get("dob") or mofl_creds.get("2FA")
            
            if not password:
                self.logger.warning("Missing PASSWORD for login")
                self.login_error_message = "Missing PASSWORD for login"
                self.login_error_code = ""
                return None
            
            login_response = self.auth_api.login(password=password, dob=dob, client_code=self.client_code)
            
            if login_response and login_response.get("status", "").upper() == "SUCCESS":
                auth_token = login_response.get("AuthToken") or login_response.get("access_token")
                if auth_token:
                    self.access_token = auth_token
                    self.jwt_token = auth_token
                    self.logger.info(f"Login successful! Token received (length: {len(auth_token)})")
                    return auth_token
                else:
                    self.logger.error("Login response missing AuthToken")
                    self.login_error_message = "Login response missing AuthToken"
                    self.login_error_code = ""
            else:
                error_msg = login_response.get("message", "Unknown error") if login_response else "No response"
                error_code = login_response.get("errorcode", "") if login_response else ""
                self.logger.error(f"Login failed: {error_msg} (code: {error_code})")
                # Store error details for publishing
                self.login_error_message = error_msg
                self.login_error_code = error_code
        except Exception as e:
            self.logger.error(f"Exception during login: {e}", exc_info=True)
            self.login_error_message = str(e)
            self.login_error_code = ""
        
        return None
    
    # ============================================================================
    # Lifecycle Management
    # ============================================================================
    
    def start(self):
        self.is_running = True
        self.logger.info(f"MotilalAdapter initialized for entity '{self.entity_id}'")
        
        if not (self.access_token or self.jwt_token):
            self.logger.info("Waiting for LOGIN. Send LOGIN action with 'jwt_token' or 'request_token' to authenticate.")

    def stop(self):
        self.is_running = False
        if self.websocket:
            self.websocket.stop()
        # Stop queue processor
        if self.queue_processor_thread and self.queue_processor_thread.is_alive():
            self.request_queue.put(None)  # Signal to stop
            self.queue_processor_thread.join(timeout=2)
        
        # Call logout API if we have auth_api and access_token
        if self.auth_api and (self.access_token or self.jwt_token):
            try:
                self.logger.info("Calling Motilal logout API...")
                logout_response = self.auth_api.logout(
                    access_token=self.access_token or self.jwt_token,
                    client_code=self.client_code
                )
                if logout_response and logout_response.get("status", "").upper() == "SUCCESS":
                    self.logger.info("Logout API call successful")
                    # Clear tokens after successful logout
                    self.access_token = None
                    self.jwt_token = None
                else:
                    error_msg = logout_response.get("message", "Unknown error") if logout_response else "No response"
                    self.logger.warning(f"Logout API call failed: {error_msg}")
            except Exception as e:
                self.logger.error(f"Exception during logout API call: {e}", exc_info=True)
        
        self._publish_logout()
        self.logger.info(f"Stopped MotilalAdapter for entity '{self.entity_id}'")
    
    # ============================================================================
    # Publishing Methods
    # ============================================================================
    
    def _publish_to_blitz_channel(self, message_type, status, message):
        try:
            event = self.formatter.response(message_type, status, message)
            self.redis_client.publish(self.response_channel, event)
            self.logger.info(f"[ADAPTER] Published {message_type} to {self.response_channel}")
        except Exception as e:
            self.logger.error(f"Failed to publish {message_type}: {e}")
    
    def _publish_login_success(self):
        self._publish_to_blitz_channel("LOGIN", "CONNECTED", "Logged in successfully")
    
    def _publish_login_failed(self, error_msg, error_code=""):
        message = error_msg
        if error_code:
            message = f"{error_msg} (code: {error_code})"
        self._publish_to_blitz_channel("LOGIN", "FAILED", message)
    
    def _publish_logout(self):
        self._publish_to_blitz_channel("LOGOUT", "SUCCESS", "Logged out successfully")
    
    # ============================================================================
    # Data Fetching
    # ============================================================================
    
    def _fetch_initial_data(self):
        try:
            if not self.order_api:
                self.logger.warning("Cannot fetch initial data: Order API not initialized")
                return
            
            self.logger.info("Fetching initial data (orders, positions, holdings) before starting WebSocket...")
            
            try:
                orders_response = self.order_api.get_orders()
                if orders_response and orders_response.get("status") == "SUCCESS":
                    orders_data = orders_response.get("Data") or []
                    if orders_data:
                        self.logger.info(f"Fetched {len(orders_data)} initial orders")
                        for order in orders_data:
                            uniqueorderid = order.get("uniqueorderid")
                            if uniqueorderid:
                                self._update_order_cache(uniqueorderid, order)
                                blitz_id = order.get("BlitzAppOrderID")
                                if blitz_id and uniqueorderid:
                                    self.blitz_to_motilal[blitz_id] = uniqueorderid
                                    self.motilal_to_blitz[uniqueorderid] = blitz_id
            except Exception as e:
                self.logger.warning(f"Failed to fetch initial orders: {e}")
            
            try:
                if self.portfolio_api:
                    positions_response = self.portfolio_api.get_positions()
                    if positions_response and positions_response.get("status") == "SUCCESS":
                        positions_data = positions_response.get("Data") or []
                        if positions_data:
                            self.logger.info(f"Fetched {len(positions_data)} initial positions")
            except Exception as e:
                self.logger.warning(f"Failed to fetch initial positions: {e}")
            
            try:
                if self.portfolio_api:
                    holdings_response = self.portfolio_api.get_holdings()
                    if holdings_response and holdings_response.get("status") == "SUCCESS":
                        holdings_data = holdings_response.get("Data") or []
                        if holdings_data:
                            self.logger.info(f"Fetched {len(holdings_data)} initial holdings")
            except Exception as e:
                self.logger.warning(f"Failed to fetch initial holdings: {e}")
            
            self.logger.info("Initial data fetch completed")
        except Exception as e:
            self.logger.error(f"Error fetching initial data: {e}")
    
    # ============================================================================
    # WebSocket Management
    # ============================================================================
    
    def _start_websocket(self):
        try:
            if not (self.access_token or self.jwt_token):
                self.logger.warning("Cannot start WebSocket: No access token available")
                return
            
            self._fetch_initial_data()
            
            self.logger.info("[ADAPTER] Starting Motilal WebSocket connection...")
            self.websocket = MotilalWebSocket(
                api_key=self.api_key,
                access_token=self.access_token or self.jwt_token,
                user_id=self.user_id or self.client_code,
                client_code=self.client_code,
                entity_id=self.entity_id,
                redis_client=self.redis_client,
                order_id_mapper=self.motilal_to_blitz,
                order_cache_callback=self._update_order_cache,
                on_connected_callback=None,
                logger=self.logger
            )
            self.websocket.start()
            self.logger.info("[ADAPTER] Motilal WebSocket started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket: {e}")

    def _is_websocket_connected(self):
        """Check if websocket is connected"""
        if not self.websocket:
            return False
        return getattr(self.websocket, 'is_connected', False)
    
    # ============================================================================
    # Rate Limiting
    # ============================================================================
    
    def _start_queue_processor(self):
        """Start background thread to process queued requests"""
        def process_queue():
            while self.is_running:
                try:
                    # Wait for item in queue (with timeout to check is_running)
                    item = self.request_queue.get(timeout=0.1)
                    if item is None:  # Stop signal
                        break
                    
                    payload, callback = item
                    
                    # Check rate limit before processing
                    while not self._check_rate_limit() and self.is_running:
                        # Wait a bit before checking again
                        time.sleep(0.1)
                    
                    if not self.is_running:
                        break
                    
                    # Process the request
                    if callback:
                        callback(payload)
                    
                    self.request_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"[RATE_LIMITER] Error processing queued request: {e}")
        
        self.queue_processor_thread = threading.Thread(target=process_queue, daemon=True)
        self.queue_processor_thread.start()
        self.logger.info("[RATE_LIMITER] Queue processor thread started")
    
    def _check_rate_limit(self):
        """Check if we can process a request now or need to queue it"""
        current_time = time.time()
        
        with self.rate_limit_lock:
            # Remove timestamps older than 1 second
            while self.request_timestamps and current_time - self.request_timestamps[0] >= self.rate_limit_window:
                self.request_timestamps.popleft()
            
            # Check if we're at the rate limit
            if len(self.request_timestamps) >= self.rate_limit:
                return False  # Need to queue
            
            # Add current timestamp
            self.request_timestamps.append(current_time)
            return True  # Can process immediately
    
    def _queue_request(self, payload, callback):
        """Queue a request to be processed later"""
        self.request_queue.put((payload, callback))
        self.logger.info(f"[RATE_LIMITER] Request queued. Queue size: {self.request_queue.qsize()}")
    
    # ============================================================================
    # Command Processing
    # ============================================================================
    
    def process_command(self, payload):
        action = payload.get("Action")
        blitz_data = payload.get("Data", {})

        self.logger.info(f"[BLITZ REQUEST] {json.dumps(payload)}")
        self.logger.info(f"Received: {action}")

        api_response = None
        error_msg = None

        # Check websocket connection for order-related actions
        order_actions = ["PLACE_ORDER", "MODIFY_ORDER", "CANCEL_ORDER", "GET_ORDERS", "GET_ORDER_DETAILS"]
        if action in order_actions:
            if not self._is_websocket_connected():
                error_msg = "WebSocket is not connected. Please wait for WebSocket to connect before placing orders."
                self.logger.warning(f"[ADAPTER] {error_msg}")
                self._publish_error_response(error_msg, blitz_data, action)
                return
            
            # Check rate limit for order actions
            if not self._check_rate_limit():
                error_msg = "Rate limit reached (10 requests per second). Request queued."
                self.logger.warning(f"[ADAPTER] {error_msg}")
                self._publish_error_response(error_msg, blitz_data, action)
                # Also queue the request for later processing
                self._queue_request(payload, lambda p: self._process_command_internal(p))
                return

        # Process the command
        self._process_command_internal(payload)
    
    def _process_command_internal(self, payload):
        action = payload.get("Action")
        blitz_data = payload.get("Data", {})
        
        api_response = None
        error_msg = None
        
        try:
            if action == "PLACE_ORDER":
                if not self.order_api:
                    raise RuntimeError("Not logged in! LOGIN first.") 
                params = MotilalMapper.to_motilal(blitz_data)
                self.logger.info(f"[API REQUEST] {json.dumps(params)}")
                try:
                    # Build place_order arguments - only include disclosedquantity if it exists and > 0
                    place_order_args = {
                        "symboltoken": params["symboltoken"],
                        "exchange": params["exchange"],
                        "side": params["side"],
                        "quantity": params["quantity"],
                        "amoorder": params["amoorder"],
                        "order_type": params["order_type"],
                        "product_type": params["product_type"],
                        "price": params["price"],
                        "trigger_price": params["trigger_price"],
                        "validity": params["validity"]
                    }
                    # Only add disclosedquantity if it exists in params (i.e., if it was > 0)
                    if "disclosedquantity" in params:
                        place_order_args["disclosedquantity"] = params["disclosedquantity"]
                    
                    api_response = self.order_api.place_order(**place_order_args)
                except Exception as api_error:
                    error_response = {"status": "ERROR", "message": str(api_error), "errorcode": ""}
                    if hasattr(api_error, 'args') and len(api_error.args) > 0:
                        error_msg = str(api_error.args[0])
                        if "API Error" in error_msg:
                            error_response["message"] = error_msg.split("]: ")[-1] if "]: " in error_msg else error_msg
                            error_response["errorcode"] = error_msg.split("[")[1].split("]")[0] if "[" in error_msg and "]" in error_msg else ""
                    
                    api_response = error_response
                    api_response.update(blitz_data)
                    api_response["orderstatus"] = "ERROR"
                    api_response["error"] = error_response["message"]
                
                order_id = None
                blitz_id = None
                try:
                    order_id = MotilalMapper.extract_order_id(api_response)
                    blitz_id = blitz_data.get("BlitzAppOrderID")
                    
                    if blitz_id and order_id:
                        self._map_order_ids(blitz_id, order_id)

                    api_response = {
                        "blitz_order_id": blitz_id,
                        "motilal_order_id": order_id,
                        "motilal_response": api_response 
                    }

                except Exception as e:
                    self.logger.error(f"Failed to process response mapping: {e}")
                
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response)}")
                
                motilal_resp = api_response.get("motilal_response", api_response) if isinstance(api_response, dict) else api_response
                if isinstance(motilal_resp, dict) and motilal_resp.get("status", "").upper() == "ERROR":
                    # API failed: publish error with status and message
                    error_msg = motilal_resp.get("message", "Unknown error")
                    error_code = motilal_resp.get("errorcode", "")
                    if error_code:
                        error_msg = f"{error_msg} (code: {error_code})"
                    self._publish_error_response(error_msg, blitz_data, action, motilal_resp)
                else:
                    # API success: cache and wait for websocket
                    if isinstance(motilal_resp, dict) and motilal_resp.get("status", "").upper() == "SUCCESS":
                        merged_response = {**params, **motilal_resp}
                        merged_response["uniqueorderid"] = motilal_resp.get("uniqueorderid")
                        merged_response["exchange"] = params.get("exchange")
                        merged_response["symboltoken"] = params.get("symboltoken")
                        merged_response["ExchangeInstrumentID"] = params.get("symboltoken")
                        merged_response["buyorsell"] = params.get("side")
                        merged_response["ordertype"] = params.get("order_type")
                        merged_response["orderqty"] = params.get("quantity")
                        merged_response["price"] = params.get("price")
                        merged_response["triggerprice"] = params.get("trigger_price")
                        merged_response["orderduration"] = params.get("validity")
                        merged_response["disclosedqty"] = params.get("disclosedquantity")
                        merged_response["producttype"] = params.get("product_type")
                        
                        if order_id:
                            self.order_cache[str(order_id)] = merged_response
                            if blitz_id:
                                self.order_cache[blitz_id] = merged_response
                            self.logger.info(f"Cached order_id={order_id}, blitz_id={blitz_id}, waiting for websocket")
                    # Websocket will publish

            elif action == "MODIFY_ORDER":
                if not self.order_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                motilal_order_id = MotilalMapper.resolve_order_id(blitz_data, self.blitz_to_motilal)
                
                # Get lastmodifiedtime from websocket cache only (not from request)
                blitz_order_id = blitz_data.get("BlitzAppOrderID")
                cached_order = self.order_cache.get(str(motilal_order_id)) or (self.order_cache.get(blitz_order_id) if blitz_order_id else None)
                
                if not cached_order:
                    self.logger.warning(f"No cached order for order_id={motilal_order_id}, waiting for websocket update")
                    raise ValueError("lastmodifiedtime required for MODIFY_ORDER. Order not found in cache. Please wait for websocket update.")
                
                # Get lastmodifiedtime from websocket cache
                prev_timestamp = cached_order.get("lastmodifiedtime") or cached_order.get("LastModifiedTime")
                traded_quantity = cached_order.get("qtytradedtoday") or cached_order.get("QtyTradedToday") or cached_order.get("tradedquantity") or 0
                
                if not prev_timestamp or prev_timestamp == "":
                    self.logger.warning(f"lastmodifiedtime missing in cache for order_id={motilal_order_id}, waiting for websocket update")
                    raise ValueError("lastmodifiedtime required for MODIFY_ORDER. Order found but lastmodifiedtime missing. Please wait for websocket update.")
                
                self.logger.info(f"Using websocket cached lastmodifiedtime={prev_timestamp} for order_id={motilal_order_id}")
                
                # Use to_motilal_modify to handle Modified* fields
                params = MotilalMapper.to_motilal_modify(blitz_data)
                
                if prev_timestamp and not isinstance(prev_timestamp, str):
                    prev_timestamp = str(prev_timestamp)
                
                # Build modify_order arguments - only include fields that exist in params
                modify_params = {
                    "order_id": motilal_order_id,
                    "order_type": params.get("order_type"),
                    "validity": params.get("validity"),
                    "price": params.get("price"),
                    "quantity": params.get("quantity"),
                    "prev_timestamp": prev_timestamp,
                    "traded_quantity": traded_quantity
                }
                # Only add disclosedquantity if it exists in params
                if "disclosedquantity" in params:
                    modify_params["disclosedquantity"] = params["disclosedquantity"]
                
                self.logger.info(f"[API REQUEST] {json.dumps(modify_params)}")
                self.logger.info(f"[MODIFY_ORDER] lastmodifiedtime={prev_timestamp}")
                
                api_response = self.order_api.modify_order(**modify_params)

                self.logger.info(f"[API RESPONSE] {json.dumps(api_response)}")
                
                motilal_resp = api_response.get("motilal_response", api_response) if isinstance(api_response, dict) else api_response
                if isinstance(motilal_resp, dict) and motilal_resp.get("status", "").upper() == "ERROR":
                    # API failed: publish error with status and message
                    error_msg = motilal_resp.get("message", "Unknown error")
                    error_code = motilal_resp.get("errorcode", "")
                    if error_code:
                        error_msg = f"{error_msg} (code: {error_code})"
                    self._publish_error_response(error_msg, blitz_data, action, motilal_resp)
                else:
                    # API success: use cached mapping, wait for websocket
                    blitz_order_id = blitz_data.get("BlitzAppOrderID")
                    
                    # Use cached mapping from PLACE_ORDER (map if missing)
                    if blitz_order_id and motilal_order_id:
                        if blitz_order_id not in self.blitz_to_motilal or str(motilal_order_id) not in self.motilal_to_blitz:
                            self.blitz_to_motilal[blitz_order_id] = str(motilal_order_id)
                            self.motilal_to_blitz[str(motilal_order_id)] = blitz_order_id
                            self.logger.info(f"Mapped: blitz_order_id={blitz_order_id} <-> uniqueorderid={motilal_order_id}")
                            
                            if self.websocket and hasattr(self.websocket, 'order_id_mapper'):
                                self.websocket.order_id_mapper[str(motilal_order_id)] = blitz_order_id
                                self.logger.info(f"Updated WebSocket order_id_mapper with: {motilal_order_id} -> {blitz_order_id}")
                        else:
                            self.logger.info(f"Using cached mapping: blitz_order_id={blitz_order_id} <-> uniqueorderid={motilal_order_id}")
                            # Ensure websocket mapper is updated
                            if self.websocket and hasattr(self.websocket, 'order_id_mapper'):
                                self.websocket.order_id_mapper[str(motilal_order_id)] = blitz_order_id
                    
                    cached_order = self.order_cache.get(str(motilal_order_id)) or (self.order_cache.get(blitz_order_id) if blitz_order_id else None)
                    
                    if cached_order:
                        # Merge cached order data with modify response
                        merged_response = {**cached_order, **api_response}
                        merged_response["uniqueorderid"] = motilal_order_id
                        # Update with modified values from request
                        if params.get("price"):
                            merged_response["price"] = params.get("price")
                        if params.get("quantity"):
                            merged_response["orderqty"] = params.get("quantity")
                            merged_response["quantityinlot"] = params.get("quantity")
                        if params.get("validity"):
                            merged_response["orderduration"] = params.get("validity")
                        if params.get("order_type"):
                            merged_response["ordertype"] = params.get("order_type")
                        # Update cache with modified order data
                        self.order_cache[str(motilal_order_id)] = merged_response
                        if blitz_order_id:
                            self.order_cache[blitz_order_id] = merged_response
                        self.logger.info(f"Updated cache for order_id={motilal_order_id}, waiting for websocket")
                    else:
                        # Cache minimal data if missing
                        merged_response = {**api_response}
                        merged_response["uniqueorderid"] = motilal_order_id
                        if params.get("symbol"):
                            merged_response["symbol"] = params.get("symbol")
                        if params.get("exchange"):
                            merged_response["exchange"] = params.get("exchange")
                        if params.get("price"):
                            merged_response["price"] = params.get("price")
                        if params.get("quantity"):
                            merged_response["orderqty"] = params.get("quantity")
                        if params.get("validity"):
                            merged_response["orderduration"] = params.get("validity")
                        if params.get("order_type"):
                            merged_response["ordertype"] = params.get("order_type")
                        self.order_cache[str(motilal_order_id)] = merged_response
                        if blitz_order_id:
                            self.order_cache[blitz_order_id] = merged_response
                        self.logger.info(f"Cached minimal data for order_id={motilal_order_id}, waiting for websocket")
                    # Websocket will publish

            elif action == "CANCEL_ORDER":
                if not self.order_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                motilal_order_id = MotilalMapper.resolve_order_id(blitz_data, self.blitz_to_motilal)
                cancel_params = {"order_id": motilal_order_id}
                self.logger.info(f"[API REQUEST] {json.dumps(cancel_params)}")
                api_response = self.order_api.cancel_order(motilal_order_id)
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response)}")
                
                motilal_resp = api_response
                if isinstance(api_response, dict) and api_response.get("status", "").upper() == "ERROR":
                    # API failed: publish error with status and message
                    error_msg = api_response.get("message", "Unknown error")
                    error_code = api_response.get("errorcode", "")
                    if error_code:
                        error_msg = f"{error_msg} (code: {error_code})"
                    self._publish_error_response(error_msg, blitz_data, action, motilal_resp)
                else:
                    # API success: use cached mapping, wait for websocket
                    blitz_order_id = blitz_data.get("BlitzAppOrderID")
                    if blitz_order_id and motilal_order_id:
                        self._map_order_ids(blitz_order_id, motilal_order_id)
                    
                    cached_order = self.order_cache.get(str(motilal_order_id)) or (self.order_cache.get(blitz_order_id) if blitz_order_id else None)
                    
                    if cached_order:
                        # Merge cached order data with cancel response
                        merged_response = {**cached_order, **api_response}
                        merged_response["uniqueorderid"] = motilal_order_id
                        # Update cache with cancelled order data
                        self.order_cache[str(motilal_order_id)] = merged_response
                        if blitz_order_id:
                            self.order_cache[blitz_order_id] = merged_response
                        self.logger.info(f"Updated cache for order_id={motilal_order_id}, waiting for websocket")
                    else:
                        # Cache minimal data if missing
                        self.order_cache[str(motilal_order_id)] = {**api_response, "uniqueorderid": motilal_order_id}
                        if blitz_order_id:
                            self.order_cache[blitz_order_id] = {**api_response, "uniqueorderid": motilal_order_id}
                        self.logger.info(f"Cached minimal data for order_id={motilal_order_id}, waiting for websocket")
                    # Websocket will publish

            elif action == "GET_ORDERS":
                if not self.order_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                self.logger.info(f"[API REQUEST] GET_ORDERS")
                api_response = self.order_api.get_orders()
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response, default=str)}")
                
                motilal_resp = api_response.get("Data", []) if isinstance(api_response, dict) else []
                if motilal_resp is None:
                    motilal_resp = []
                if isinstance(motilal_resp, list):
                    for order in motilal_resp:
                        order_id = order.get("uniqueorderid") or order.get("orderid")
                        if order_id:
                            self.order_cache[str(order_id)] = order
                            blitz_id = self.motilal_to_blitz.get(str(order_id))
                            if blitz_id:
                                self.order_cache[blitz_id] = order
                
                blitz_response = MotilalMapper.to_blitz(api_response, "orders", entity_id=self.entity_id, action_name=action)
                if blitz_response:
                    self.logger.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response, default=self._serialize_orderlog)}")
                    self._publish_response(blitz_response, motilal_response=api_response)

            elif action == "GET_ORDER_DETAILS":
                if not self.order_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                motilal_order_id = MotilalMapper.resolve_order_id(blitz_data, self.blitz_to_motilal)
                order_details_params = {"order_id": motilal_order_id}
                self.logger.info(f"[API REQUEST] {json.dumps(order_details_params)}")
                api_response = self.order_api.get_order_by_id(motilal_order_id)
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response, default=str)}")
                
                blitz_response = MotilalMapper.to_blitz([api_response], "orders", entity_id=self.entity_id, action_name=action)
                if blitz_response:
                    self.logger.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response, default=self._serialize_orderlog)}")
                    self._publish_response(blitz_response, motilal_response=api_response)

            elif action == "GET_HOLDINGS":
                if not self.portfolio_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                self.logger.info(f"[API REQUEST] GET_HOLDINGS")
                api_response = self.portfolio_api.get_holdings()
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response, default=str)}")

                blitz_response = MotilalMapper.to_blitz(api_response, "holdings", entity_id=self.entity_id, action_name=action)
                if blitz_response:
                    self.logger.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response, default=self._serialize_orderlog)}")
                    self._publish_response(blitz_response, motilal_response=api_response)

            elif action == "GET_POSITIONS":
                if not self.portfolio_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                self.logger.info(f"[API REQUEST] GET_POSITIONS")
                api_response = self.portfolio_api.get_positions()
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response, default=str)}")
                
                blitz_response = MotilalMapper.to_blitz(api_response, "positions", entity_id=self.entity_id, action_name=action)
                if blitz_response:
                    self.logger.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response, default=self._serialize_orderlog)}")
                    self._publish_response(blitz_response, motilal_response=api_response)

            elif action == "LOGIN":
                jwt_token = blitz_data.get("jwt_token") or blitz_data.get("AuthToken") or blitz_data.get("access_token")
                password = blitz_data.get("PASSWORD") or blitz_data.get("password")
                dob = blitz_data.get("DOB") or blitz_data.get("dob") or blitz_data.get("2FA")
                
                if jwt_token:
                    self.jwt_token = jwt_token
                    self.access_token = jwt_token
                    self.logger.info("Using provided jwt_token/AuthToken")
                elif password:
                    if not self.auth_api:
                        self.auth_api = MotilalAuthAPI(api_key=self.api_key, client_code=self.client_code)
                    
                    login_response = self.auth_api.login(password=password, dob=dob, client_code=self.client_code)
                    if login_response and login_response.get("status", "").upper() == "SUCCESS":
                        auth_token = login_response.get("AuthToken") or login_response.get("access_token")
                        if auth_token:
                            self.jwt_token = auth_token
                            self.access_token = auth_token
                            self.logger.info("Login successful via LOGIN action")
                        else:
                            raise ValueError("Login response missing AuthToken")
                    else:
                        error_msg = login_response.get("message", "Login failed") if login_response else "Login failed"
                        raise ValueError(f"Login failed: {error_msg}")
                else:
                    raise ValueError("Missing 'jwt_token', 'AuthToken', or 'PASSWORD' in data")
                
                self.order_api = MotilalOswalOrderAPI(
                    api_key=self.api_key,
                    client_code=self.client_code,
                    jwt_token=self.jwt_token,
                    api_secret_key=self.api_secret,
                    access_token=self.access_token
                )
                self.portfolio_api = MotilalPortfolioAPI(
                    access_token=self.access_token,
                    api_key=self.api_key,
                    client_code=self.client_code
                )
                
                self._start_websocket()
                
                api_response = {"status": "SUCCESS", "access_token": self.access_token, "jwt_token": self.jwt_token, "message": "Logged in successfully"}
                self.logger.info(f"[API RESPONSE] {json.dumps(api_response, default=str)}")
            else:
                self.logger.warning(f"Action '{action}' not implemented in automated mode")
                
        except Exception as e:
            self.logger.error(f" !! Error executing {action}: {e}")
            error_msg = str(e)
            self.logger.info(f"[API RESPONSE] status=FAILED | error={error_msg}")
            
            # Publish FAILED response using error_to_orderlog for all actions
            self._publish_error_response(error_msg, blitz_data, action)
    
    # ============================================================================
    # Helper Methods
    # ============================================================================
    
    def _publish_error_response(self, error_msg, blitz_data, action, motilal_response=None):
        """Common method to publish error responses"""
        error_data = []
        if blitz_data:
            error_data.append(blitz_data)
        if motilal_response:
            error_data.append(motilal_response)
        order_log = MotilalMapper.error_to_orderlog(error_msg, error_data, self.entity_id)
        blitz_response = MotilalMapper.to_blitz([order_log], "orders", entity_id=self.entity_id, action_name=action)
        if blitz_response:
            self.logger.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response, default=self._serialize_orderlog)}")
            self._publish_response(blitz_response, motilal_response=motilal_response)

    def _map_order_ids(self, blitz_id, motilal_id):
        """Common method to create bidirectional order ID mapping"""
        if blitz_id and motilal_id:
            motilal_id_str = str(motilal_id)
            if blitz_id not in self.blitz_to_motilal or motilal_id_str not in self.motilal_to_blitz:
                self.blitz_to_motilal[blitz_id] = motilal_id_str
                self.motilal_to_blitz[motilal_id_str] = blitz_id
                self.logger.info(f"Mapped: {blitz_id} <-> {motilal_id}")
                if self.websocket and hasattr(self.websocket, 'order_id_mapper'):
                    self.websocket.order_id_mapper[motilal_id_str] = blitz_id
            else:
                self.logger.info(f"Using cached mapping: {blitz_id} <-> {motilal_id}")
                if self.websocket and hasattr(self.websocket, 'order_id_mapper'):
                    self.websocket.order_id_mapper[motilal_id_str] = blitz_id

    def _update_order_cache(self, order_id, order_data):
        if order_id and order_data:
            self.order_cache[str(order_id)] = order_data
            blitz_id = self.motilal_to_blitz.get(str(order_id))
            if blitz_id:
                self.order_cache[blitz_id] = order_data

    def _serialize_orderlog(self, obj):
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def _publish_response(self, blitz_response, motilal_response=None):
        try:
            self.redis_client.publish(self.response_channel, blitz_response, default=self._serialize_orderlog)
        except Exception as e:
            self.logger.error(f"Failed to publish response: {e}")
