import json
import logging
import threading
import time
import sys
import os

sys.path.append(os.getcwd())

try:
    import websocket
except ImportError:
    websocket = None

from Motilal.motilal_mapper import MotilalMapper
from common.broker_order_mapper import OrderLog
from common.logging_setup import get_entity_logger
from common.message_formatter import MessageFormatter
from common.redis_client import RedisClient

logger = logging.getLogger("MOFL")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[MOFL] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class MotilalWebSocket:
    
    WS_URL = "wss://openapi.motilaloswal.com/ws"

    def __init__(self, api_key, access_token, user_id, client_code=None, entity_id=None, redis_client=None, callback_func=None, order_id_mapper=None, order_cache_callback=None, on_connected_callback=None, logger=None):
        self.api_key = api_key
        self.client_id = user_id or client_code
        self.auth_token = access_token
        self.entity_id = entity_id
        self.callback_func = callback_func
        if redis_client and isinstance(redis_client, RedisClient):
            self.redis_client = redis_client
        else:
            self.redis_client = RedisClient()
        self.order_id_mapper = order_id_mapper if order_id_mapper is not None else {}
        self.order_cache_callback = order_cache_callback
        self.on_connected_callback = on_connected_callback
        self.logger = logger or get_entity_logger(entity_id) if entity_id else logger

        self.ws = None
        self._thread = None
        self._heartbeat_thread = None
        self._should_run = False
        self._reconnect_count = 0
        self._max_reconnect_delay = 60
        self._auth_failed = False
        self._auth_success = False
        self.is_connected = False
        self._tpoms_connected_published = False
        self._heartbeat_interval = 30
        
        self.subscription_types = [
            "OrderSubscribe",      # Order updates 
            "TradeSubscribe",       # Trade updates
            # "PositionSubscribe",    # Position updates
            # "HoldingSubscribe",     # Holding updates
        ]

        import config
        self.CH_BLITZ_RESPONSES = config.CH_BLITZ_RESPONSES
        self.CH_BLITZ_REQUESTS = config.CH_BLITZ_REQUESTS
    

    def start(self):
        if websocket is None:
            self.logger.error("websocket-client package is not installed; cannot start MOFL WebSocket")
            return

        if not (self.api_key and self.client_id and self.auth_token):
            self.logger.error(f"Missing credentials for MOFL WebSocket (api_key/client_id/auth_token) for entity={self.entity_id}")
            return

        self._should_run = True

        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._thread = threading.Thread(
            target=lambda: self.ws.run_forever(ping_interval=30, ping_timeout=10),
            daemon=True
        )
        self._thread.start()
        self.logger.info(f"[WEBSOCKET] WebSocket thread started for entity={self.entity_id} client_id={self.client_id} (with keepalive ping every 30s)")

    def stop(self):
        self._should_run = False
        self.is_connected = False
        self._stop_heartbeat()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
    
    def _start_heartbeat(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        
        def heartbeat_loop():
            while self._should_run:
                time.sleep(self._heartbeat_interval)
                if self._should_run and self.is_connected and self.ws:
                    try:
                        heartbeat_msg = {
                            "action": "heartbeat",
                            "clientid": str(self.client_id)
                        }
                        
                        self._send_json(heartbeat_msg)
                        self.logger.info(f"[WEBSOCKET] Sent heartbeat for entity={self.entity_id}")
                    except Exception as e:
                        self.logger.error(f"[WEBSOCKET] Error sending heartbeat for entity={self.entity_id}: {e}")
        
        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        self.logger.info(f"[WEBSOCKET] Heartbeat thread started for entity={self.entity_id} (interval: {self._heartbeat_interval}s)")
    
    def _stop_heartbeat(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self.logger.info(f"[WEBSOCKET] Heartbeat thread will stop naturally for entity={self.entity_id}")

    def _publish(self, channel, message):
        try:
            result = self.redis_client.publish(channel, message, default=str)
            if result:
                self.logger.info(f"[WEBSOCKET] Successfully published message to {channel} for entity={self.entity_id}")
            else:
                self.logger.warning(f"[WEBSOCKET] Failed to publish message to {channel} for entity={self.entity_id} (RedisClient returned False)")
        except Exception as e:
            self.logger.error(f"Error publishing to Redis for entity={self.entity_id}: {e}", exc_info=True)
            if self.callback_func:
                try:
                    self.callback_func(channel, message)
                    self.logger.info(f"[WEBSOCKET] Published via callback function for entity={self.entity_id}")
                except Exception as callback_error:
                    self.logger.error(f"Error in MOFL WebSocket callback for entity={self.entity_id}: {callback_error}")

    def _send_json(self, payload: dict):
        try:
            if self.ws:
                action = payload.get("action", "N/A")
                self.logger.info(f"[WEBSOCKET] Sending action: {action}, payload: {json.dumps(payload)}")
                self.ws.send(json.dumps(payload))
        except Exception as e:
            self.logger.error(f"Failed to send payload on MOFL WebSocket for entity={self.entity_id}: {e}")

    def _subscribe_to_all(self):
        """Subscribe to all available subscription types"""
        for sub_type in self.subscription_types:
            try:
                sub_msg = {
                    "clientid": str(self.client_id),
                    "action": sub_type,
                }
                self._send_json(sub_msg)
                self.logger.info(f"[WEBSOCKET] Sent {sub_type} message for entity={self.entity_id}")
            except Exception as e:
                self.logger.error(f"[WEBSOCKET] Failed to send {sub_type} for entity={self.entity_id}: {e}")
    
    def subscribe(self, subscription_type):
        """Manually subscribe to a specific subscription type"""
        if subscription_type not in self.subscription_types:
            self.logger.warning(f"[WEBSOCKET] Unknown subscription type: {subscription_type} for entity={self.entity_id}")
        try:
            sub_msg = {
                "clientid": str(self.client_id),
                "action": subscription_type,
            }
            self._send_json(sub_msg)
            self.logger.info(f"[WEBSOCKET] Sent manual {subscription_type} message for entity={self.entity_id}")
        except Exception as e:
            self.logger.error(f"[WEBSOCKET] Failed to send manual {subscription_type} for entity={self.entity_id}: {e}")
    
    def add_subscription_type(self, subscription_type):
        """Add a custom subscription type to the list"""
        if subscription_type not in self.subscription_types:
            self.subscription_types.append(subscription_type)
            self.logger.info(f"[WEBSOCKET] Added custom subscription type: {subscription_type} for entity={self.entity_id}")

    def _on_open(self, ws):
        self._reconnect_count = 0
        self.is_connected = True
        self.logger.info(f"[WEBSOCKET] WebSocket connected for entity={self.entity_id}, sending auth...")
        auth_msg = {
            "clientid": self.client_id,
            "authtoken": self.auth_token,
            "apikey": self.api_key,
        }
        self.logger.info(f"[WEBSOCKET] Sending auth message: {json.dumps({**auth_msg, 'authtoken': auth_msg['authtoken'][:20] + '...' if auth_msg['authtoken'] else None})}")
        self._send_json(auth_msg)
        # Subscriptions will be sent after auth success in _on_message
        
        formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
        event = formatter.system_event("CONNECTED", source="motilal_websocket", entity_id=self.entity_id)
        self._publish(self.CH_BLITZ_RESPONSES, event)
        
        ws_connected = formatter.connection_status("CONNECTED", "WebSocket connected successfully")
        self._publish(self.CH_BLITZ_RESPONSES, ws_connected)
        
        if not self._tpoms_connected_published:
            formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
            tpoms_connected = formatter.connection_status("CONNECTED", "Login and WebSocket connected successfully")
            self._publish(self.CH_BLITZ_RESPONSES, tpoms_connected)
            self._tpoms_connected_published = True
            self.logger.info(f"[WEBSOCKET] Published WEBSOCKET_CONNECTED and TPOMS_CONNECT CONNECTED to {self.CH_BLITZ_RESPONSES}")
            
            if self.on_connected_callback:
                try:
                    self.logger.info(f"[WEBSOCKET] Calling on_connected_callback for entity={self.entity_id}")
                    self.on_connected_callback()
                except Exception as e:
                    self.logger.error(f"[WEBSOCKET] Error in on_connected_callback: {e}")
        else:
            self.logger.info(f"[WEBSOCKET] Published WEBSOCKET_CONNECTED (TPOMS_CONNECT CONNECTED already published)")
        
        self._start_heartbeat()
        
    def _on_message(self, ws, message):
        self.logger.info(f"[WEBSOCKET] ===== _on_message CALLED ===== entity={self.entity_id}")
        self.logger.info(f"[WEBSOCKET] Raw message received for entity={self.entity_id}: {message}")
        
        try:
            msg_data = json.loads(message)
            formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
            
            if msg_data.get("status"):
                status = msg_data.get("status", "").upper()
                message_text = msg_data.get("message", "")
                error_code = msg_data.get("errorcode", "")
                
                if status == "ERROR":
                    error_code_str = f" (code: {error_code})" if error_code else ""
                    formatted_msg = formatter.response("WEBSOCKET_ERROR", "FAILED", f"{message_text}{error_code_str}", entity_id=self.entity_id)
                    self.logger.info(f"[WEBSOCKET] Publishing ERROR message to {self.CH_BLITZ_RESPONSES}: {json.dumps(formatted_msg, default=str)}")
                    self._publish(self.CH_BLITZ_RESPONSES, formatted_msg)
                    
                    error_code_str_check = str(error_code)
                    if "MO2014" in error_code_str_check or "Invalid User" in message_text:
                        self.logger.error(f"[WEBSOCKET] Authentication failed for entity={self.entity_id}: {message_text}")
                        self.logger.error(f"[WEBSOCKET] Credentials check: client_id='{self.client_id}', api_key='{self.api_key[:10] if self.api_key else None}...', auth_token length={len(self.auth_token) if self.auth_token else 0}")
                        self._auth_failed = True
                        self._should_run = False
                        if self.ws:
                            try:
                                self.ws.close()
                            except Exception:
                                pass
                    return
                elif status == "SUCCESS":
                    # Check if this is auth success
                    if "auth" in message_text.lower() or "success" in message_text.lower():
                        if not self._auth_success:
                            self._auth_success = True
                            self.logger.info(f"[WEBSOCKET] Auth successful, subscribing to all types for entity={self.entity_id}")
                            self._subscribe_to_all()
                    
                    formatted_msg = formatter.response("WEBSOCKET_MESSAGE", "SUCCESS", message_text, entity_id=self.entity_id)
                    self.logger.info(f"[WEBSOCKET] Publishing SUCCESS message to {self.CH_BLITZ_RESPONSES}: {json.dumps(formatted_msg, default=str)}")
                    self._publish(self.CH_BLITZ_RESPONSES, formatted_msg)
                    return
            
            if msg_data.get("uniqueorderid") or msg_data.get("orderid"):
                order_id = msg_data.get("uniqueorderid") or msg_data.get("orderid")
                self.logger.info(f"[WEBSOCKET] Received order update: uniqueorderid={order_id}, status={msg_data.get('orderstatus', 'N/A')}")
                self.logger.info(f"[WEBSOCKET] Full order data from websocket: {json.dumps(msg_data, default=str)}")
                
                if order_id and self.order_cache_callback:
                    self.order_cache_callback(order_id, msg_data)
                
                order_log = OrderLog()
                MotilalMapper._map_order(msg_data, order_log, entity_id=self.entity_id)
                
                if order_id:
                    blitz_id = self.order_id_mapper.get(str(order_id))
                    if blitz_id:
                        order_log.BlitzOrderId = blitz_id
                        self.logger.info(f"[WEBSOCKET] Order ID Mapping found: motilal_id={order_id} -> blitz_id={blitz_id}")
                    else:
                        self.logger.warning(f"[WEBSOCKET] No mapping found for uniqueorderid={order_id}, available mappings: {list(self.order_id_mapper.keys())}")
                
                formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
                blitz_response = formatter.order_update(order_log, entity_id=self.entity_id)
                self.logger.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response, default=str)}")
                self.logger.info(f"[WEBSOCKET] Publishing order update to {self.CH_BLITZ_RESPONSES} for order_id={order_id}")
                self._publish(self.CH_BLITZ_RESPONSES, blitz_response)
                return
            
            self.logger.info(f"[WEBSOCKET] Received unhandled message type, publishing as raw message: {json.dumps(msg_data, default=str)}")
            formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
            message_type = msg_data.get("messageType") or msg_data.get("action") or "WEBSOCKET_MESSAGE"
            message_text = json.dumps(msg_data, default=str)
            blitz_response = formatter.response(message_type, "SUCCESS", message_text, entity_id=self.entity_id)
            self._publish(self.CH_BLITZ_RESPONSES, blitz_response)
            
        except json.JSONDecodeError as e:
            self.logger.warning(f"[WEBSOCKET] Received non-JSON message: {message[:200]}...")
            formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
            blitz_response = formatter.response("WEBSOCKET_MESSAGE", "SUCCESS", message, entity_id=self.entity_id)
            self._publish(self.CH_BLITZ_RESPONSES, blitz_response)
        except Exception as e:
            self.logger.error(f"[WEBSOCKET] Error processing message for entity={self.entity_id}: {e}", exc_info=True)

    def _on_error(self, ws, error):
        self.logger.error(f"error: {error} (entity={self.entity_id})")
        formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
        event = formatter.system_event("ERROR", source="motilal_websocket", error=str(error), entity_id=self.entity_id)
        self._publish(self.CH_BLITZ_RESPONSES, event)

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        self.logger.warning(f"closed: {code} {reason} (entity={self.entity_id})")
        
        formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
        event = formatter.system_event("DISCONNECTED", source="motilal_websocket", code=code, reason=reason, entity_id=self.entity_id)
        self._publish(self.CH_BLITZ_RESPONSES, event)
        
        ws_disconnected = formatter.connection_status("DISCONNECTED", f"WebSocket disconnected (code: {code}, reason: {reason})")
        self._publish(self.CH_BLITZ_RESPONSES, ws_disconnected)
        
        if self._tpoms_connected_published:
            tpoms_disconnected = formatter.connection_status("DISCONNECTED", f"WebSocket disconnected (code: {code}, reason: {reason})")
            self._publish(self.CH_BLITZ_RESPONSES, tpoms_disconnected)
            self._tpoms_connected_published = False
            self.logger.info(f"[WEBSOCKET] Published WEBSOCKET_DISCONNECTED and TPOMS_CONNECT DISCONNECTED to {self.CH_BLITZ_RESPONSES}")
        else:
            self.logger.info(f"[WEBSOCKET] Published WEBSOCKET_DISCONNECTED to {self.CH_BLITZ_RESPONSES}")

        if self._should_run and not self._auth_failed:
            import time
            
            def reconnect():
                self._reconnect_count += 1
                delay = min(3 * (2 ** (self._reconnect_count - 1)), self._max_reconnect_delay)
                
                if not self._should_run or self._auth_failed:
                    return
                
                self.logger.info(f"Reconnecting WebSocket for entity={self.entity_id} in {delay} seconds (attempt {self._reconnect_count})...")
                time.sleep(delay)
                
                if not self._should_run or self._auth_failed:
                    return
                
                try:
                    self.ws = websocket.WebSocketApp(
                        self.WS_URL,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    reconnect_thread = threading.Thread(
                        target=lambda: self.ws.run_forever(ping_interval=30, ping_timeout=10),
                        daemon=True
                    )
                    reconnect_thread.start()
                except Exception as e:
                    self.logger.error(f"Failed to reconnect WebSocket for entity={self.entity_id}: {e}")
            
            reconnect_thread = threading.Thread(target=reconnect, daemon=True)
            reconnect_thread.start()
        elif self._auth_failed:
            self.logger.error(f"Stopping reconnection attempts for entity={self.entity_id} due to authentication failure. Please check credentials.")
