import json
import sys
import os
import threading
import time

#from scrpicode import MotilalScripAPI

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.request_handler import RequestHandler

from Motilal.motilal_mapper import MotilalMapper
from Motilal.api.order import MotilalOswalOrderAPI
from Motilal.api.portfolio import MotilalPortfolioAPI
from Motilal.api.auth import MotilalAuthAPI
from Motilal.motilal_websocket import MotilalWebSocket
from common.message_formatter import MessageFormatter
from common.redis_client import RedisClient
from common.broker_order_mapper import OrderLog



class MotilalAdapter:
    def __init__(self, entity_id=None, creds=None, logger= None):
        # -------------------------
        # Basic setup & logging
        # -------------------------
        self.entity_id = entity_id
        self.creds = creds or {}
        self.logger = logger

        # -------------------------
        # Internal caches & mappings
        # -------------------------
        self.blitz_order_cache = {}
        self.blitz_to_motilal = {}
        self.motilal_to_blitz = {}
        self.blitz_order_action={}
        self.adapter_published_ids = {}
        self._pending_place_request = {}
        
        self.websocket = None
        self.is_running = True
        self.start_resync_loop(interval=30) 

        # -------------------------
        # Extract credentials
        # -------------------------
        mofl_creds = self._extract_mofl_credentials(creds)
        self.api_key = mofl_creds.get("ApiKey")
        self.password = mofl_creds.get("Password")
        self.dob = mofl_creds.get("DOB")
        self.client_code = mofl_creds.get("ClientId")
        self.access_token = None
        self.jwt_token = None

       
        # -------------------------
        # Redis client & channels
        # -------------------------
        self.redis_client = RedisClient()
        
        self.logger.info(f"Initializing MotilalAdapter for entity '{entity_id}'")

        # -------------------------
        # Auth API & instant login
        # -------------------------
        if self.api_key and self.client_code and self.password:
            self.auth_api = MotilalAuthAPI(
                api_key=self.api_key,
                client_code=self.client_code,
                password=self.password,
                dob=self.dob,
                logger = self.logger
            )
            self.access_token = self.auth_api.access_token
            self.jwt_token = self.auth_api.auth_token

            if self.access_token:
                self.logger.info("Login successful! Token received from auth API.")
                # self._publish_login_success()

            else:
                self.logger.warning("Login failed. Adapter may not work properly.")

            # # -------------------------
            # # Download NSEFO scrip master
            # # -------------------------
           
            # scrip_api = MotilalScripAPI(self.api_key, self.client_code, self.access_token)
            # result = scrip_api.get_scrips("NSEFO")
            # print(json.dumps(result, indent=2))
            # filename = "scrip.json"

            # with open(filename, "w") as f:
            #     json.dump(result, f, indent=2)

            # print(f"script saved to {filename}")
              
        # -------------------------
        # Initialize Order & Portfolio APIs
        # -------------------------
        if self.access_token or self.jwt_token:
            self.order_api = MotilalOswalOrderAPI(
                api_key=self.api_key,
                client_code=self.client_code,
                jwt_token=self.jwt_token or self.access_token,
                access_token=self.access_token or self.jwt_token,
                logger= self.logger
            )
            self.portfolio_api = MotilalPortfolioAPI(
                access_token=self.access_token or self.jwt_token,
                api_key=self.api_key,
                client_code=self.client_code
            )
            self.logger.info("Order and Portfolio APIs initialized. Ready to connect.")
        else:
            self.order_api = None
            self.portfolio_api = None
            self.logger.info("No token available. Adapter not ready.")

        # -------------------------
        # Message formatter
        # -------------------------
        self.formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)

    # -------------------------
    # Credential helpers
    # -------------------------
    def _extract_mofl_credentials(self, creds):
        if not creds:
            return {}
        if isinstance(creds, dict) and "MOFL" in creds:
            return creds["MOFL"]
        return creds

    
    # -------------------------
    # Data helpers
    # -------------------------
    @staticmethod
    def _blitz_field(data, *keys, default=None):
        """Blitz field lookup (case-insensitive)."""
        if not data or not isinstance(data, dict):
            return default
        for k in keys:
            if k in data:
                return data[k]
        return default

    def _response_field(self, result, blitz_data, field_name, default=""):
        """Prefer result; fallback to blitz_data."""
        if isinstance(result, dict) and field_name in result:
            v = result[field_name]
            if v is not None and (v != "" if isinstance(v, str) else True):
                return v
        return self._blitz_field(blitz_data, field_name, default=default)

    # -------------------------
    # WebSocket
    # -------------------------
    def _start_websocket(self):
        if not (self.access_token or self.jwt_token):
            self.logger.warning("Cannot start WebSocket: No access token available")
            return

        try:
            self.logger.info("[ADAPTER] Starting Motilal WebSocket connection...")
            self.websocket = MotilalWebSocket(
                api_key=self.api_key,
                access_token=self.access_token or self.jwt_token,
                client_code=self.client_code,
                entity_id=self.entity_id,
                redis_client=self.redis_client.connection,
                order_id_mapper=self.motilal_to_blitz,
                blitz_order_cache=self.blitz_order_cache,
                blitz_order_action= self.blitz_order_action,
                adapter_published_ids=self.adapter_published_ids,
                pending_place_request=self._pending_place_request,
                logger=self.logger
                
                
            )
            self.websocket.start()
            self.logger.info("[ADAPTER] Motilal WebSocket started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket: {e}")

    # -------------------------
    # Stop adapter
    # -------------------------
    def stop(self):
        self.is_running = False
        if self.auth_api and (self.access_token or self.jwt_token):
            try:
                self.auth_api.logout(
                    access_token=self.access_token or self.jwt_token,
                    client_code=self.client_code,
                )
            except Exception as e:
                self.logger.warning(f"Logout API call failed: {e}")
        if self.websocket:
            self.websocket.stop()

        self.logger.info(f"Stopped MotilalAdapter for entity '{self.entity_id}'")

    def resync_unpublished_orders(self):
        """
        Re-publish orders which were not published to Blitz
        """
        #self.logger.info("[RESYNC] Checking for unpublished orders")
        if not self.blitz_order_action or not any(
            action is not None for action in self.blitz_order_action.values()
        ):
            #self.logger.info("[RESYNC] No pending actions, skipping get_orders()")
            return

        try:
            api_response = self.order_api.get_orders()
        except Exception as e:
            self.logger.error(f"[RESYNC] GET_ORDERS failed: {e}")
            return

        orders = api_response.get("data", []) if isinstance(api_response, dict) else []
        if not orders:
            self.logger.info("[RESYNC] No orders from API")
            return

        for order in orders:
            order_id = str(order.get("uniqueorderid"))
            blitz_id = self.motilal_to_blitz.get(order_id)

            if not blitz_id:
                continue

            action = self.blitz_order_action.get(blitz_id)
            cached_data = self.blitz_order_cache.get(blitz_id)
            if cached_data is None:
                continue

            last_modifiedtime = order.get("lastmodifiedtime")
            cached_data["LastModifiedDateTime"] = last_modifiedtime
            self.blitz_order_cache[blitz_id] = cached_data
            

            # Action already consumed → nothing to resync
            if action is None:
                #self.logger.info(f"[RESYNC] Action already consumed for blitz_id={blitz_id}")
                continue

            # Create an OrderLog object for mapping
            order_log = OrderLog()

            # Handle cached_data being dict or OrderLog object
            if cached_data is not None and not isinstance(cached_data, dict):
                # Convert OrderLog to dict temporarily for mapping
                cached_dict = cached_data.to_dict()
            else:
                cached_dict = cached_data or {}

            MotilalMapper.map_order(order, order_log, cached_dict, action)

            if not order_log.OrderStatus:
                self.logger.info(
                    f"[RESYNC] Skipping unmapped status "
                    f"{order.get('orderstatus')} for blitz_id={blitz_id}"
                )
                continue

            # Publish to Blitz
            blitz_response = self.formatter.orders(
                [order_log],
                entity_id=self.entity_id,
                message_type="RE_SYNC"
            )

            data = blitz_response.get("Data")
            if data:
                self.redis_client.publish(json.dumps(data[0], default=str))
                self.logger.info(f"[RESYNC] Published blitz_id={blitz_id}, action={action}")
                # Consume action after successful publish
                self.blitz_order_action[blitz_id] = None



    def start_resync_loop(self, interval):
        def loop():
            while self.is_running:
                try:
                    self.resync_unpublished_orders()
                except Exception as e:
                    self.logger.error(f"[RESYNC LOOP] Error: {e}")
                time.sleep(interval)

        threading.Thread(target=loop, daemon=True).start()

    def handle_place_order(self, blitz_data, action):
        """Handle PLACE_ORDER action with clean error/success mapping."""
        if not self.order_api:
            raise RuntimeError("Not logged in! LOGIN first.")
        
        self.logger.info("[BLITZ-INBOUND] PLACE_ORDER - Parameters: %s",
            json.dumps(blitz_data, default=str)
        )

        # -------------------------
        # STEP 1: Cache Blitz Data only
        # -------------------------
        blitz_id = blitz_data.get("BlitzAppOrderID")
        if blitz_id:
            self.blitz_order_cache[blitz_id] = blitz_data
            self.blitz_order_action[blitz_id] = action
            self.logger.info("Blitz data cached: %s", blitz_id)

        self._pending_place_request["data"] = blitz_data
        params = MotilalMapper.to_motilal(blitz_data)

        self.logger.info(
            "[TPOMS-OUTBOUND][API] PLACE_ORDER - Parameters: %s",
            json.dumps(params, default=str)
        )

        # -------------------------
        # STEP 2: Call API
        # -------------------------
        try:
            api_response = self.order_api.place_order(
                symboltoken=params["symboltoken"],
                exchange=params["exchange"],
                side=params["side"],
                quantity=params["quantity"],
                amoorder=params["amoorder"],
                order_type=params["order_type"],
                product_type=params["product_type"],
                price=params["price"],
                trigger_price=params["trigger_price"],
                validity=params["validity"],
                disclosedquantity=params.get("disclosedquantity", 0),
                tag=params.get("tag")
            )

        # -------------------------
        # STEP 3: API ERROR HANDLING
        # -------------------------
       
        except Exception as api_error:
            api_err = RequestHandler.extract_api_error(api_error)

            message = api_err.get("message", "Order rejected")
            order_id = api_err.get("uniqueorderid")
            status = api_err.get("status", "Rejected")

            # ------------------------------
            # CASE 1: Order ID EXISTS 
            # ------------------------------
            if order_id:
                if blitz_id:
                    self.blitz_to_motilal[blitz_id] = str(order_id)
                    self.motilal_to_blitz[str(order_id)] = blitz_id
                    if self.websocket:
                        self.websocket.order_id_mapper[str(order_id)] = blitz_id

                order_log = MotilalMapper.error_to_orderlog(
                    message,
                    blitz_data,
                    status,
                    action
                )

            # ------------------------------
            # CASE 2: NO Order ID → HARD REJECTION
            # ------------------------------
            else:
                order_log = OrderLog.orderlog_error(
                    error_msg=message,
                    blitz_data=blitz_data,
                    err_status="Rejected",
                    action=action
                )

            # ------------------------------
            # ALWAYS publish to Blitz
            # ------------------------------
            #self.blitz_order_cache[blitz_id] = order_log

            blitz_response = self.formatter.orders(
                [order_log],
                entity_id=self.entity_id,
                message_type=action
            )
            order_data = blitz_response["Data"][0]
            self.redis_client.publish( json.dumps(order_data))
            self.blitz_order_action[blitz_id] = None

            self.logger.error(
                "[PLACE_ORDER ERROR] %s",
                json.dumps(blitz_response, default=self.serialize_orderlog)
            )

            return


        # -------------------------
        # STEP 4: SUCCESS HANDLING
        # -------------------------
        self.logger.info("PLACE_ORDER accepted by API")

        order_id = MotilalMapper.extract_order_id(api_response)

        # Map Blitz ↔ Motilal IDs
        if blitz_id and order_id:
            self.blitz_to_motilal[blitz_id] = str(order_id)
            self.motilal_to_blitz[str(order_id)] = blitz_id
            if self.websocket:
                self.websocket.order_id_mapper[str(order_id)] = blitz_id

            self.logger.info("Mapped: blitz_id=%s <-> uniqueorderid=%s",blitz_id,order_id)

        # WebSocket will create/update OrderLog and publish
        return

    def handle_modify_order(self, blitz_data, action):
        """Handle MODIFY_ORDER without mapping again."""

        if not self.order_api:
            raise RuntimeError("Not logged in! LOGIN first.")
        
        self.logger.info("[BLITZ-INBOUND] MODIFY_ORDER - Parameters: %s",json.dumps(blitz_data, default=str))


        blitz_id = blitz_data.get("BlitzAppOrderID")
        order_id = self.blitz_to_motilal.get(blitz_id)

        self.blitz_order_action[blitz_id] = action
        cashed_data = self.blitz_order_cache.get(blitz_id)

        params = MotilalMapper.to_motilal_modify(blitz_data,cashed_data, order_id)
        self.logger.info(
            "[TPOMS-OUTBOUND][API] MODIFY_ORDER - Parameters: %s",
            json.dumps(blitz_data, default=str)
        )

       
        # -------------------------
        # API CALL
        # -------------------------
        try:
            api_response = self.order_api.modify_order(
            order_id=order_id,
            order_type=params["newordertype"],
            validity=params["neworderduration"],
            price=params.get("newprice", 0),
            quantity=params["newquantityinlot"],
            prev_timestamp=params["lastmodifiedtime"],
            traded_quantity=params.get("traded_quantity", 0),
            
        )


        # -------------------------
        # API ERROR → create OrderLog
        # -------------------------
        except Exception as api_error: 
            api_err = RequestHandler.extract_api_error(api_error)
            massage = api_err.get("message")
            #order_id = api_err.get("uniqueorderid")
            status = api_err.get("status")
            
            
            cached_data = self.blitz_order_cache.get(blitz_id)
            if not cached_data:
                self.logger.error(f"[CACHE MISS] No cached data for {blitz_id}")
                return
            self.blitz_order_cache[blitz_id] 
            # Create OrderLog ONLY for API error
            order_log = MotilalMapper.error_to_orderlog(massage,cached_data, status,action)

           
            #self.blitz_order_cache[blitz_id] = order_log

            
            blitz_response = self.formatter.orders(
                [order_log], 
                entity_id=self.entity_id,
                message_type=action
            )

            order_data = blitz_response["Data"][0]
            self.redis_client.publish(json.dumps(order_data, default=str))
            self.logger.info(blitz_response)
            self.blitz_order_action[blitz_id] = None


            # self.redis_client.publish(json.dumps(order_data))
            self.logger.info(f"API Error on Modifieng Order {(blitz_response.get("Data"))}")
            return

        # -------------------------
        # SUCCESS → WebSocket will update
        # -------------------------
        self.logger.info(f"Modify accepted, waiting for WebSocket update: {api_response}")

    def handle_cancel_order(self, blitz_data, action):
        """Handle CANCEL_ORDER without remapping or success OrderLog creation."""

        if not self.order_api:
            raise RuntimeError("Not logged in! LOGIN first.")
        self.logger.info(
            "[BLITZ-INBOUND] CANCEL_ORDER - Parameters: %s",
            json.dumps(blitz_data, default=str)
        )


        blitz_id = blitz_data.get("BlitzAppOrderID")
       
        self.blitz_order_action[blitz_id] = action
        cashed_data = self.blitz_order_cache.get(blitz_id)
        
        motilal_order_id = self.blitz_to_motilal.get(blitz_id)
       
        self.logger.info(
            f"[TPOMS-OUTBOUND][API] CANCEL_ORDER - "
            f"{json.dumps({'order_id': motilal_order_id}, default=str)}"
        )

        # -------------------------
        # API CALL
        # -------------------------
        try:
            api_response = self.order_api.cancel_order(motilal_order_id)

        # -------------------------
        # API ERROR → create OrderLog
        # -------------------------
        except Exception as api_error: 
            api_err = RequestHandler.extract_api_error(api_error)
            massage = api_err.get("message")
            #order_id = api_err.get("uniqueorderid")
            status = api_err.get("status")
            
            
            cached = self.blitz_order_cache.get(blitz_id)
            if not cached:
                self.logger.error(f"[CACHE MISS] No cached data for {blitz_id}")
                return

            # Create OrderLog ONLY for API error
            order_log = MotilalMapper.error_to_orderlog(massage,blitz_data, status, action)


            # self.blitz_order_cache[blitz_id] = order_log

            blitz_response = self.formatter.orders(
                [order_log],
                entity_id=self.entity_id,
                message_type=action
            )

            self.redis_client.publish(blitz_response.get("Data"))
            self.logger.info(f"Api Error on Cancellation Oredr {blitz_response.get("Data")})")
            #self.blitz_order_action[blitz_id] = None
            
            return

        # -------------------------
        # SUCCESS → WebSocket will update
        # -------------------------
        self.logger.info(
            f"Cancel accepted by API ={api_response}, "
            f"waiting for WebSocket update"
        )


    def handle_get_orders(self, action):
        if not self.order_api:
            raise RuntimeError("Not logged in! LOGIN first.")
        self.logger.info(f"[MOTILAL API REQUEST] GET_ORDERS - No parameters")
        api_response = self.order_api.get_orders()
        self.logger.info(f"[MOTILAL API RESPONSE] GET_ORDERS - Full response: {json.dumps(api_response, default=str)}")
        if isinstance(api_response, dict) and api_response.get("status", "").upper() == "ERROR":
            self.logger.error(f"[MOTILAL API RESPONSE] GET_ORDERS - Status: ERROR, Message: {api_response.get('message', 'Unknown error')}")
        else:
            self.logger.info(f"[MOTILAL API RESPONSE] GET_ORDERS - Status: SUCCESS, Orders count: {len(api_response.get('data', [])) if isinstance(api_response, dict) else 0}")
        motilal_resp = api_response.get("data", []) if isinstance(api_response, dict) else []
        if motilal_resp is None:
            motilal_resp = []
        if isinstance(motilal_resp, list):
            for order in motilal_resp:
                order_id = order.get("uniqueorderid") or order.get("orderid")
                if order_id:
                    self.blitz_order_cache[str(order_id)] = order
                    blitz_id = self.motilal_to_blitz.get(str(order_id))
                    if blitz_id:
                        self.blitz_order_cache[blitz_id] = order
        order_logs = []
        if isinstance(api_response, dict) and "data" in api_response:
            data = api_response.get("data", [])
            if isinstance(data, list):
                for item in data:
                    order_log = OrderLog()
                    MotilalMapper._map_order(item, order_log)
                    order_logs.append(order_log)
            elif isinstance(data, dict):
                order_log = OrderLog()
                MotilalMapper._map_order(data, order_log)
                order_logs.append(order_log)
        elif isinstance(api_response, list):
            for item in api_response:
                order_log = OrderLog()
                MotilalMapper._map_order(item, order_log)
                order_logs.append(order_log)
        elif isinstance(api_response, dict):
            order_log = OrderLog()
            MotilalMapper._map_order(api_response, order_log)
            order_logs.append(order_log)
        for o in order_logs:
            o.SequenceNumber = 0
        blitz_response = self.formatter.orders(order_logs, entity_id=self.entity_id, message_type=action) if order_logs else None
        if blitz_response:
            self.logger.info(f"[BLITZ RESPONSE] GET_ORDERS - Response: {json.dumps(blitz_response, default=self.serialize_orderlog)}")
            self.redis_client.publish(blitz_response)

    def handle_get_trades(self, action):
        if not self.order_api:
            raise RuntimeError("Not logged in! LOGIN first.")
        self.logger.info("[MOTILAL API REQUEST] GET_TRADES - No parameters")
        api_response = self.order_api.get_tradebook()
        self.logger.info(f"[MOTILAL API RESPONSE] GET_TRADES - Full response: {json.dumps(api_response, default=str)}")
        if isinstance(api_response, dict) and api_response.get("status", "").upper() == "ERROR":
            self.logger.error(f"[MOTILAL API RESPONSE] GET_TRADES - Status: ERROR, Message: {api_response.get('message', 'Unknown error')}")
        else:
            trades_count = len(api_response.get("data", [])) if isinstance(api_response, dict) else 0
            self.logger.info(f"[MOTILAL API RESPONSE] GET_TRADES - Status: SUCCESS, Trades count: {trades_count}")
        data = api_response.get("data", []) if isinstance(api_response, dict) else []
        if data is None or not isinstance(data, list):
            data = []
        blitz_response = self.formatter.trades(data, entity_id=self.entity_id, message_type=action)
        self.logger.info(f"[BLITZ RESPONSE] GET_TRADES - Response: {json.dumps(blitz_response, default=str)}")
        self.redis_client.publish(blitz_response)
        
    

    def process_command(self, payload):
        action = payload.get("Action")
        blitz_data = payload.get("Data") or {}
        self.logger.info(f"[BLITZ-INBOUND] Action={action}, Full payload: {json.dumps(payload, default=str)}")
        
        try:
            # ---------------- Orders ----------------
            if action in ["PLACE_ORDER", "MODIFY_ORDER", "CANCEL_ORDER"]:
                handler_map = {
                    "PLACE_ORDER": self.handle_place_order,
                    "MODIFY_ORDER": self.handle_modify_order,
                    "CANCEL_ORDER": self.handle_cancel_order
                }
                handler = handler_map.get(action)
                if handler:
                    handler(blitz_data, action)
                return

            # ---------------- GET_ORDERS / GET_TRADES ----------------
            elif action == "GET_ORDERS":
                self.handle_get_orders(action)
                return
            elif action == "GET_TRADES":
                self.handle_get_trades(action)
                return

            # ---------------- GET_ORDER_DETAILS ----------------
            elif action == "GET_ORDER_DETAILS":
                if not self.order_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                motilal_order_id = MotilalMapper.resolve_order_id(blitz_data, self.blitz_to_motilal)
                self.logger.info(f"[MOTILAL API REQUEST] GET_ORDER_DETAILS - Parameters: {{'order_id': {motilal_order_id}}}")
                api_response = self.order_api.get_order_by_id(motilal_order_id)
                self.logger.info(f"[MOTILAL API RESPONSE] GET_ORDER_DETAILS - Full response: {json.dumps(api_response, default=str)}")
                if isinstance(api_response, dict) and api_response.get("status", "").upper() == "ERROR":
                    self.logger.error(f"[MOTILAL API RESPONSE] GET_ORDER_DETAILS - Status: ERROR, Message: {api_response.get('message', 'Unknown error')}")
                order_log = OrderLog()
                MotilalMapper._map_order(api_response, order_log)
                blitz_response = self.formatter.orders([order_log], entity_id=self.entity_id, message_type=action)
                if blitz_response:
                    self.logger.info(f"[BLITZ RESPONSE] GET_ORDER_DETAILS - Response: {json.dumps(blitz_response, default=self.serialize_orderlog)}")
                    self.redis_client.publish(blitz_response)
                return

            # ---------------- GET_HOLDINGS ----------------
            elif action == "GET_HOLDINGS":
                if not self.portfolio_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                self.logger.info(f"[MOTILAL API REQUEST] GET_HOLDINGS - No parameters")
                api_response = self.portfolio_api.get_holdings()
                self.process_portfolio_response(api_response, action, mapper=MotilalMapper._map_holding, formatter_func=self.formatter.holdings)
                return

            # ---------------- GET_POSITIONS ----------------
            elif action == "GET_POSITIONS":
                if not self.portfolio_api:
                    raise RuntimeError("Not logged in! LOGIN first.")
                self.logger.info(f"[MOTILAL API REQUEST] GET_POSITIONS - No parameters")
                api_response = self.portfolio_api.get_positions()
                self.process_portfolio_response(api_response, action, mapper=MotilalMapper._map_position, formatter_func=self.formatter.positions)
                return

            else:
                self.logger.warning(f"Action '{action}' not implemented in automated mode")

        except Exception as e:
            self.logger.error(f"[ERROR] Error executing {action}: {e}", exc_info=True)
            error_msg = str(e)
           

# ------------------ Helper for holdings/positions ----------------
    def process_portfolio_response(self, api_response, action, mapper, formatter_func):
        if isinstance(api_response, dict) and api_response.get("status", "").upper() == "ERROR":
            self.logger.error(f"[MOTILAL API RESPONSE] {action} - Status: ERROR, Message: {api_response.get('message', 'Unknown error')}")
            data_list = []
        else:
            data_list = api_response.get("data") if isinstance(api_response, dict) else api_response
            if not isinstance(data_list, list):
                data_list = [data_list]
            self.logger.info(f"[MOTILAL API RESPONSE] {action} - Status: SUCCESS, Count: {len(data_list)}")

        mapped_data = [mapper(d) for d in data_list]
        blitz_response = formatter_func(mapped_data, entity_id=self.entity_id, message_type=action) if mapped_data else None
        if blitz_response:
            self.logger.info(f"[BLITZ RESPONSE] {action} - Response: {json.dumps(blitz_response, default=self.serialize_orderlog)}")
            self.redis_client.publish(blitz_response)

   
    # def mark_adapter_published(self, order_id, blitz_id):
    #     """Mark order as adapter-published (WS skips duplicate)."""
    #     if order_id:
    #         self.adapter_published_ids.add(str(order_id))
    #     if blitz_id:
    #         self.adapter_published_ids.add(str(blitz_id))


    def serialize_orderlog(self, obj):
            if hasattr(obj, 'to_dict'):
                return obj.to_dict()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
