import json
import logging
import threading
import time
import sys
import os

from common.broker_order_mapper import OrderLog

sys.path.append(os.getcwd())

try:
    import websocket
except ImportError:
    websocket = None

from Motilal.motilal_mapper import MotilalMapper
from common.message_formatter import MessageFormatter
from common.redis_client import RedisClient
import config


# ---------------------------------------------------------
# Logger
# ---------------------------------------------------------
# logger = logging.getLogger("MOFL")
# if not logger.handlers:
#     _handler = logging.StreamHandler()
#     _handler.setFormatter(logging.Formatter("[MOFL] %(message)s"))
#     logger.addHandler(_handler)

# logger.setLevel(logging.INFO)
# logger.propagate = False


# ---------------------------------------------------------
# WebSocket Class
# ---------------------------------------------------------
class MotilalWebSocket:
    WS_URL = "wss://openapi.motilaloswal.com/ws"

    def __init__(
        self,
        api_key,
        access_token,
        client_code=None,
        entity_id=None,
        redis_client=None,
        callback_func=None,
        order_id_mapper=None,
        order_callback=None,
        blitz_order_cache = None,
        blitz_order_action= None,
        on_connected_callback=None,
        logger=None,
        adapter_published_ids=None,
        pending_place_request=None,
    ):
        # -------------------------
        # Credentials & identity
        # -------------------------
        self.api_key = api_key
        self.auth_token = access_token
        self.client_id = client_code
        self.entity_id = entity_id
   
        # -------------------------
        # Logger & redis
        # -------------------------
        self.logger = logger 
        self.redis_client = redis_client if isinstance(redis_client, RedisClient) else RedisClient()

        # -------------------------
        # Callbacks & state
        # -------------------------
        self.callback_func = callback_func
        self.order_callback = order_callback
        self.on_connected_callback = on_connected_callback

        # -------------------------
        # Order mappings & cache
        # -------------------------
        self.order_id_mapper = order_id_mapper
        self.blitz_order_cache = blitz_order_cache
        self.blitz_order_action = blitz_order_action
        self.adapter_published_ids = adapter_published_ids
        self.pending_place_request = pending_place_request or {}

        # -------------------------
        # Runtime flags
        # -------------------------
        self.ws = None
        self._thread = None
        self._heartbeat_thread = None
        self._should_run = False
        self._reconnect_count = 0
        self._max_reconnect_delay = 60

        self.is_connected = False
        self._auth_failed = False
        self._auth_success = False
        self._tpoms_connected_published = False

        # -------------------------
        # Heartbeat
        # -------------------------
        self._heartbeat_interval = 30

        # -------------------------
        # Subscriptions
        # -------------------------
        self.subscription_types = [
            "OrderSubscribe",
            "TradeSubscribe",
        ]

        self.CH_BLITZ_RESPONSES = config.CH_BLITZ_RESPONSES
        self.CH_BLITZ_REQUESTS = config.CH_BLITZ_REQUESTS

    # ---------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------
    def start(self):
        if websocket is None:
            self.logger.error("websocket-client package not installed")
            return

        if not (self.api_key and self.client_id and self.auth_token):
            self.logger.error(
                f"Missing credentials for MOFL WebSocket (api_key/client_id/auth_token) "
                f"entity={self.entity_id}"
            )
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
            daemon=True,
        )
        self._thread.start()

        self.logger.info(
            f"[WEBSOCKET] Started for entity={self.entity_id}, client_id={self.client_id}"
        )

    def stop(self):
        self._should_run = False
        self.is_connected = False
        self._stop_heartbeat()

        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    # ---------------------------------------------------------
    # Heartbeat
    # ---------------------------------------------------------
    def _start_heartbeat(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return

        def heartbeat_loop():
            while self._should_run:
                time.sleep(self._heartbeat_interval)
                if self._should_run and self.is_connected and self.ws:
                    try:
                        self._send_json({
                            "action": "heartbeat",
                            "clientid": str(self.client_id),
                        })
                        self.logger.info(
                            f"[WEBSOCKET] Heartbeat sent entity={self.entity_id}"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"[WEBSOCKET] Heartbeat error entity={self.entity_id}: {e}"
                        )

        self._heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            daemon=True
        )
        self._heartbeat_thread.start()

        self.logger.info(
            f"[WEBSOCKET] Heartbeat started (interval={self._heartbeat_interval}s)"
        )

    def _stop_heartbeat(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self.logger.info(
                f"[WEBSOCKET] Heartbeat stopping entity={self.entity_id}"
            )

    # ---------------------------------------------------------
    # Send helpers
    # ---------------------------------------------------------
    def _send_json(self, payload: dict):
        try:
            if self.ws:
                self.logger.info(
                    f"[WEBSOCKET] Sending {payload.get('action')}: {json.dumps(payload)}"
                )
                self.ws.send(json.dumps(payload))
        except Exception as e:
            self.logger.error(
                f"[WEBSOCKET] Send failed entity={self.entity_id}: {e}"
            )

    def _subscribe_to_all(self):
        for sub_type in self.subscription_types:
            self._send_json({
                "clientid": str(self.client_id),
                "action": sub_type,
            })

    # ---------------------------------------------------------
    # WebSocket callbacks
    # ---------------------------------------------------------
    def _on_open(self, ws):
        self._reconnect_count = 0
        self.is_connected = True

        self.logger.info(
            f"[WEBSOCKET] Connected entity={self.entity_id}, sending auth"
        )

        auth_msg = {
            "clientid": self.client_id,
            "authtoken": self.auth_token,
            "apikey": self.api_key,
        }

        self._send_json(auth_msg)

        if not self._tpoms_connected_published:
            self._tpoms_connected_published = True

            if self.on_connected_callback:
                try:
                    self.on_connected_callback()
                except Exception as e:
                    self.logger.error(
                        f"[WEBSOCKET] on_connected_callback error: {e}",
                        exc_info=True
                    )

        self._start_heartbeat()


    def _on_message(self, ws, message):

        self.logger.info("[WEBSOCKET] _on_message received")
        ws_msg = json.dumps(json.loads(message), default=str)
        self.logger.info(f"[TPOMS-INBOUND][WebSocket] Incoming message:{ws_msg}")
        
        try:
            msg_data = json.loads(message)
            formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)

            # -------------------------------------------------
            # Auth / Status Messages
            # -------------------------------------------------
            status = msg_data.get("status")
            if status:
                status = status.upper()
                message_text = msg_data.get("message", "")
                error_code = msg_data.get("errorcode", "")

                if status == "ERROR":
                    error_suffix = f" (code: {error_code})" if error_code else ""
                    error_msg = formatter.response(
                        "WEBSOCKET_ERROR",
                        "FAILED",
                        f"{message_text}{error_suffix}",
                        entity_id=self.entity_id,
                    )

                    self.logger.info(f"[WEBSOCKET] ERROR: {json.dumps(error_msg, default=str)}")
                    return

                if status == "SUCCESS":
                    if not self._auth_success and "auth" in message_text.lower():
                        self._auth_success = True
                        self.logger.info(f"[WEBSOCKET] Auth success, subscribing entity={self.entity_id}")
                        self._subscribe_to_all()
                    return
            # -------------------------------------------------
            # Order Updates
            # -------------------------------------------------
            order_id = msg_data.get("uniqueorderid")
            blitz_id = msg_data.get("tag")
            if order_id:
                self.logger.info(
                    f"[WEBSOCKET] Order update received: {order_id}, "
                    f"status={msg_data.get('orderstatus')}"
                )

                if self.order_callback:
                    self.order_callback(order_id, msg_data)

                blitz_id = MotilalMapper.resolve_order_id(
                    id_mapping=self.order_id_mapper,
                    direction="MOTILAL_TO_BLITZ",
                    order_id=order_id,
                )

                if not blitz_id:
                    blitz_id = msg_data.get("tag")
                    if blitz_id:
                        self.logger.warning(
                            f"[WEBSOCKET] Auto-creating mapping "
                            f"{order_id} -> {blitz_id}"
                        )

                        # Store both directions
                        self.order_id_mapper[str(order_id)] = str(blitz_id)
                        self.order_id_mapper[str(blitz_id)] = str(order_id)
                    else:
                        self.logger.warning(
                            f"[WEBSOCKET] No Blitz ID available to create mapping for order_id={order_id}"
                        )
                        return


                action = self.blitz_order_action.get(blitz_id)
                cached_data = self.blitz_order_cache.get(blitz_id)

                if action is None:
                    self.logger.info(f"[WEBSOCKET] Action already consumed for blitz_id={blitz_id}")

                order_log = OrderLog()
                MotilalMapper.map_order(msg_data, order_log, cached_data, action)


                if cached_data is None:
                    cached_data = {}
                # elif not isinstance(cached_data, dict):
                   
                #     cached_data = {
                #         k: getattr(cached_data, k)
                #         for k in dir(cached_data)
                # if not k.startswith("_") and not callable(getattr(cached_data, k))
                elif not isinstance(cached_data, dict):
                    cached_data = {
                        k: getattr(cached_data, k)
                        for k in dir(cached_data)
                        if not k.startswith("_") and not callable(getattr(cached_data, k))
                    }

                last_modified = msg_data.get("lastmodifiedtime")
                cached_data["LastModifiedDateTime"] = last_modified

                # Update the cache
                self.blitz_order_cache[blitz_id] = cached_data

                self.logger.info(f"[WEBSOCKET] Updated cache for blitz_id={blitz_id}")


                if not order_log.OrderStatus:
                    self.logger.info(
                        f"[WEBSOCKET] Skipping unmapped status "
                        f"{msg_data.get('orderstatus')} for order_id={order_id}"
                    )
                    return

                #order_log.BlitzAppOrderID = str(blitz_id)

                order_data = order_log.to_dict()
                blitz_response = formatter.order_update(
                    order_data,
                    entity_id=self.entity_id,
                )
                
                self.logger.info(f"Published BY WEBSOCKET")
                self.redis_client.publish(blitz_response.get("Data"))
                # self.logger.info(f"[WebSocket->Motilal] {ws_msg}")

                self.logger.info(
                    f"[BLITZ-OUTBOUND][WEBSOCKET] Publishing order update: "
                    f"{json.dumps(blitz_response, default=str)}"
                )
                self.blitz_order_action[blitz_id] = None

                return

            message_type = (
                msg_data.get("messageType")
                or msg_data.get("action")
                or "WEBSOCKET_MESSAGE"
            )

            fallback = formatter.response(
                message_type,
                "SUCCESS",
                json.dumps(msg_data, default=str),
                entity_id=self.entity_id,
            )

            
            self.redis_client.publish(fallback)

        except Exception as e:
            self.logger.error(
                f"[WEBSOCKET] Message processing error: {e}",
                exc_info=True,
            )


    def _on_error(self, ws, error):
        self.logger.error(
            f"[WEBSOCKET] Error: {error} (entity={self.entity_id})"
        )

        formatter = MessageFormatter(tpoms_name="MOFL", entity_id=self.entity_id)
        formatter.system_event(
            "ERROR",
            source="[WS]MOFL",
            error=str(error),
            entity_id=self.entity_id,
        )


    def _on_close(self, ws, code, reason):
        self.is_connected = False
        self.logger.warning(
            f"[WEBSOCKET] Closed: code={code}, reason={reason}, "
            f"entity={self.entity_id}"
        )

        if self._tpoms_connected_published:
            self._tpoms_connected_published = False

        # -------------------------------------------------
        # Reconnect Logic
        # -------------------------------------------------
        if not self._should_run or self._auth_failed:
            self.logger.error(
                "[WEBSOCKET] Reconnect stopped due to auth failure"
            )
            return

        def reconnect():
            self._reconnect_count += 1
            delay = min(
                3 * (2 ** (self._reconnect_count - 1)),
                self._max_reconnect_delay,
            )

            self.logger.info(
                f"[WEBSOCKET] Reconnecting in {delay}s "
                f"(attempt {self._reconnect_count})"
            )
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
                threading.Thread(
                    target=lambda: self.ws.run_forever(
                        ping_interval=30,
                        ping_timeout=10,
                    ),
                    daemon=True,
                ).start()
            except Exception as e:
                self.logger.error(
                    f"[WEBSOCKET] Reconnect failed: {e}"
                )

        threading.Thread(target=reconnect, daemon=True).start()

        
    
