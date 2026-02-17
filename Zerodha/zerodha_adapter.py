# ============================================================
# Standard imports
# ============================================================
from typing import Any, Dict, Optional

# ============================================================
# Project imports
# ============================================================
from common import config
from common.redis_client import RedisClient
from common.message_formatter import MessageFormatter
from common.logging_setup import EntityLogger
from common.broker_order_mapper import OrderLog

# Zerodha imports (websocket imported later to avoid circular import)
from Zerodha.auth_api import get_access_token, ZerodhaInstruments
from Zerodha.orders import ZerodhaOrderAPI


"""
Zerodha Mapper (Production – FIXED)

Responsibilities:
- Blitz → Zerodha payload (PLACE / MODIFY)
- Zerodha → Blitz OrderLog
- Error → Rejected / ReplaceRejected / CancelRejected
- Resolve Blitz ↔ Zerodha order IDs
"""


class ZerodhaMapper:
    # ==========================================================
    # Blitz → Zerodha (PLACE)
    # ==========================================================

    @staticmethod
    def to_zerodha(
        blitz_data: Dict,
        instruments_api=None
    ) -> Dict:
        """
        Convert Blitz PLACE_ORDER → Zerodha payload
        """

        exchange_segment = blitz_data.get("ExchangeSegment")
        instrument_token = blitz_data.get("ExchangeInstrumentID")

        # --------------------------------------------------
        # Resolve tradingsymbol (STRICT)
        # --------------------------------------------------
        tradingsymbol = None

        # Equity
        if exchange_segment == "NSECM":
            tradingsymbol = blitz_data.get("SymbolName")

        # Derivatives (NO FALLBACK ALLOWED)
        elif exchange_segment in ("NSEFO", "BSEFO"):
            if not instruments_api:
                raise ValueError("Instruments API not available for derivatives")

            if not instrument_token:
                raise ValueError("ExchangeInstrumentID missing for derivatives")

            tradingsymbol = instruments_api.get_tradingsymbol(
                int(instrument_token)
            )

        if not tradingsymbol:
            raise ValueError(
                f"Failed to resolve tradingsymbol | "
                f"Segment={exchange_segment} Token={instrument_token}"
            )

        return {
            "tradingsymbol": tradingsymbol,
            "exchange": ZerodhaMapper._map_exchange(exchange_segment),
            "transaction_type": blitz_data["OrderSide"].upper(),
            "order_type": ZerodhaMapper._map_order_type(blitz_data["OrderType"]),
            "quantity": int(blitz_data["OrderQuantity"]),
            "product": blitz_data["ProductType"],
            "price": float(blitz_data.get("LimitPrice") or 0),
            "trigger_price": float(blitz_data.get("StopPrice") or 0),
            "validity": ZerodhaMapper._map_validity(blitz_data.get("TimeInForce")),
            "disclosed_quantity": int(blitz_data.get("DisclosedQuantity") or 0),
        }

    # ==========================================================
    # Blitz → Zerodha (MODIFY)
    # ==========================================================

    @staticmethod
    def to_zerodha_modify(
        blitz_data: Dict,
        original_request: Dict
    ) -> Dict:
        """
        Convert Blitz MODIFY_ORDER → Zerodha modify payload
        """

        params = {}

        # Quantity
        if blitz_data.get("ModifiedOrderQuantity") is not None:
            params["quantity"] = int(blitz_data["ModifiedOrderQuantity"])

        # Price
        if blitz_data.get("ModifiedLimitPrice") is not None:
            params["price"] = float(blitz_data["ModifiedLimitPrice"])

        # Trigger price
        if blitz_data.get("ModifiedStopPrice") is not None:
            params["trigger_price"] = float(blitz_data["ModifiedStopPrice"])

        # Validity
        if blitz_data.get("ModifiedTimeInForce"):
            params["validity"] = ZerodhaMapper._map_validity(
                blitz_data["ModifiedTimeInForce"]
            )

        # Zerodha REQUIRES these always
        params["order_type"] = ZerodhaMapper._map_order_type(
            original_request["OrderType"]
        )

        params["disclosed_quantity"] = int(
            blitz_data.get("ModifiedDisclosedQuantity")
            if blitz_data.get("ModifiedDisclosedQuantity") is not None
            else original_request.get("DisclosedQuantity", 0)
        )

        if not params:
            raise ValueError("No modifiable fields provided")

        return params

    # ==========================================================
    # Zerodha → Blitz
    # ==========================================================

    @staticmethod
    def to_blitz_orderlog(
        zerodha_data: Dict,
        blitz_request: Optional[Dict] = None,
    ) -> OrderLog:

        o = OrderLog()

        # Preserve Blitz fields
        if blitz_request:
            o.BlitzAppOrderID = blitz_request.get("BlitzAppOrderID", "")
            o.Account = blitz_request.get("Account", "")
            o.ExchangeClientID = blitz_request.get("ExchangeClientID", "")
            o.ExchangeSegment = blitz_request.get("ExchangeSegment", "")
            o.ExchangeInstrumentID = blitz_request.get("ExchangeInstrumentID", 0)
            o.OrderSide = blitz_request.get("OrderSide", "")
            o.OrderType = blitz_request.get("OrderType", "")
            o.ProductType = blitz_request.get("ProductType", "")
            o.TimeInForce = blitz_request.get("TimeInForce", "")
            o.OrderQuantity = (blitz_request.get("OrderQuantity"))
            o.OrderPrice = (blitz_request.get("LimitPrice"))
            o.OrderStopPrice = (blitz_request.get("StopPrice"))
            o.OrderDisclosedQuantity = (blitz_request.get("DisclosedQuantity"))

        # Zerodha fields
        o.ExchangeOrderID = zerodha_data.get("order_id", "")
        o.OrderStatus = ZerodhaMapper._map_status(zerodha_data.get("status"))
        o.CumulativeQuantity = (zerodha_data.get("filled_quantity"))
        o.LeavesQuantity = (zerodha_data.get("pending_quantity"))
        o.OrderAverageTradedPrice = zerodha_data.get("average_price")
        o.LastUpdateDateTime = zerodha_data.get("exchange_update_timestamp", "")
        o.ExchangeTransactTime = zerodha_data.get("exchange_timestamp", "")
        o.LastTradedPrice = zerodha_data.get("average_price")
        o.LastTradedQuantity = zerodha_data.get("filled_quantity")
        o.LastExecutionTransactTime = zerodha_data.get("exchange_update_timestamp", "")
        o.ExecutionID = zerodha_data.get("exchange_order_id")
        o.CancelRejectReason = (
            zerodha_data.get("status_message")
            or zerodha_data.get("status_message_raw")
            or ""
        )

        return o

    # ==========================================================
    # Order ID Resolver
    # ==========================================================

    @staticmethod
    def resolve_order_id(
        blitz_data: Dict,
        blitz_to_zerodha: Dict[str, str],
    ) -> str:

        exchange_order_id = blitz_data.get("ExchangeOrderID")
        if exchange_order_id:
            return str(exchange_order_id)

        blitz_id = blitz_data.get("BlitzAppOrderID")
        if not blitz_id:
            raise ValueError("Missing BlitzAppOrderID")

        zerodha_order_id = blitz_to_zerodha.get(str(blitz_id))
        if not zerodha_order_id:
            raise ValueError(f"No mapping for BlitzAppOrderID {blitz_id}")

        return str(zerodha_order_id)

    # ==========================================================
    # Error → OrderLog helpers (UNCHANGED, CORRECT)
    # ==========================================================

    @staticmethod
    def error_to_orderlog(error_msg: str, blitz_data: Optional[Dict] = None, entity_id=None) -> OrderLog:
        o = OrderLog()
        o.EntityId = entity_id or ""
        o.OrderStatus = "Rejected"
        o.ExecutionType = "Rejected"
        o.CancelRejectReason = error_msg

        if blitz_data:
            o.BlitzAppOrderID = blitz_data.get("BlitzAppOrderID", "")
            o.Account = blitz_data.get("Account", "")
            o.ExchangeClientID = blitz_data.get("ExchangeClientID", "")
            o.ExchangeSegment = blitz_data.get("ExchangeSegment", "")
            o.ExchangeInstrumentID = blitz_data.get("ExchangeInstrumentID", 0)
            o.OrderSide = blitz_data.get("OrderSide", "")
            o.OrderType = blitz_data.get("OrderType", "")
            o.ProductType = blitz_data.get("ProductType", "")
            o.TimeInForce = blitz_data.get("TimeInForce", "")
            o.OrderQuantity = int(blitz_data.get("OrderQuantity") or 0)
            o.OrderPrice = float(blitz_data.get("LimitPrice") or 0)
            o.OrderStopPrice = float(blitz_data.get("StopPrice") or 0)
            o.OrderDisclosedQuantity = int(blitz_data.get("DisclosedQuantity") or 0)

        return o

    @staticmethod
    def error_to_orderlog_modify_rejected(error_msg: str, original_request_data: Optional[Dict] = None, entity_id=None) -> OrderLog:
        o = OrderLog()
        o.EntityId = entity_id or ""
        o.OrderStatus = "ReplaceRejected"
        o.ExecutionType = "ReplaceRejected"
        o.CancelRejectReason = error_msg

        if original_request_data:
            o.BlitzAppOrderID = original_request_data.get("BlitzAppOrderID", 0)
            o.Account = original_request_data.get("Account", "")
            o.ExchangeClientID = original_request_data.get("ExchangeClientID", "")
            o.ExchangeSegment = original_request_data.get("ExchangeSegment", "")
            o.ExchangeInstrumentID = original_request_data.get("ExchangeInstrumentID", 0)
            o.OrderSide = original_request_data.get("OrderSide", "")
            o.OrderType = original_request_data.get("OrderType", "")
            o.ProductType = original_request_data.get("ProductType", "")
            o.TimeInForce = original_request_data.get("TimeInForce", "")
            o.OrderQuantity = int(original_request_data.get("OrderQuantity") or 0)
            o.OrderPrice = float(original_request_data.get("LimitPrice") or 0)
            o.OrderStopPrice = float(original_request_data.get("StopPrice") or 0)
            o.OrderDisclosedQuantity = int(original_request_data.get("DisclosedQuantity") or 0)

        return o

    @staticmethod
    def error_to_orderlog_cancel_rejected(error_msg: str, original_request_data: Optional[Dict] = None, entity_id=None) -> OrderLog:
        o = OrderLog()
        o.EntityId = entity_id or ""
        o.OrderStatus = "CancelRejected"
        o.ExecutionType = "CancelRejected"
        o.CancelRejectReason = error_msg

        if original_request_data:
            o.BlitzAppOrderID = original_request_data.get("BlitzAppOrderID", 0)
            o.Account = original_request_data.get("Account", "")
            o.ExchangeClientID = original_request_data.get("ExchangeClientID", "")
            o.ExchangeSegment = original_request_data.get("ExchangeSegment", "")
            o.ExchangeInstrumentID = original_request_data.get("ExchangeInstrumentID", 0)
            o.OrderSide = original_request_data.get("OrderSide", "")
            o.OrderType = original_request_data.get("OrderType", "")
            o.ProductType = original_request_data.get("ProductType", "")
            o.TimeInForce = original_request_data.get("TimeInForce", "")
            o.OrderQuantity = int(original_request_data.get("OrderQuantity") or 0)
            o.OrderPrice = float(original_request_data.get("LimitPrice") or 0)
            o.OrderStopPrice = float(original_request_data.get("StopPrice") or 0)
            o.OrderDisclosedQuantity = int(original_request_data.get("DisclosedQuantity") or 0)

        return o

    # ==========================================================
    # Helpers
    # ==========================================================

    @staticmethod
    def _map_exchange(segment: str) -> str:
        return {
            "NSECM": "NSE",
            "NSEFO": "NFO",
            "BSECM": "BSE",
            "BSEFO": "BFO",
        }.get(segment, "NSE")

    @staticmethod
    def _map_order_type(order_type: str) -> str:
        return {
            "LIMIT": "LIMIT",
            "MARKET": "MARKET",
            "STOPLIMIT": "SL",
            "STOPMARKET": "SL-M",
        }.get(order_type, order_type)

    @staticmethod
    def _map_validity(tif: Optional[str]) -> str:
        return {
            "GFD": "DAY",
            "IOC": "IOC",
        }.get(str(tif).upper() if tif else "", "DAY")

    @staticmethod
    def _map_status(status: Optional[str]) -> str:
        return {
            "OPEN": "New",
            "COMPLETE": "Filled",
            "MODIFIED": "Replaced",
            "CANCELLED": "Cancelled",
            "REJECTED": "Rejected",
            "TRIGGER PENDING": "New",

        }.get(str(status).upper(), str(status))

# ============================================================
# Import ZerodhaWebSocket AFTER ZerodhaMapper to avoid circular import
# ============================================================
from Zerodha.zerodha_websocket import ZerodhaWebSocket

"""
Zerodha (KITE) Adapter – Clean Production Version

Responsibilities:
- Auto-login (TOTP)
- Load instruments CSV
- Start WebSocket (called by TPOMS)
- Handle PLACE / MODIFY / CANCEL orders
- Cache Blitz request data (temp + permanent)
- Proper error handling via mapper
"""




class ZerodhaAdapter:
    """
    Zerodha Adapter controlled by TPOMS lifecycle.
    """

    def __init__(
        self,
        entity_id: str,
        creds: Dict[str, Any],
        logger: Optional[EntityLogger] = None,
    ):
        self.entity_id = entity_id
        self.creds = creds or {}

        # --------------------------------------------------
        # LOGGER (one file per user)
        # --------------------------------------------------
        self.logger = logger or EntityLogger(
            broker="KITE",
            entity=entity_id,
            component="Adapter"
        )
        self.logger.info("Adapter INIT | Zerodha adapter starting")

        # --------------------------------------------------
        # Credentials
        # --------------------------------------------------
        self.api_key = self.creds.get("ApiKey")
        self.api_secret = self.creds.get("ApiSecret")
        self.user_id = self.creds.get("UserId")
        self.password = self.creds.get("Password")
        self.totp_secret = self.creds.get("TotpSecret")

        # --------------------------------------------------
        # Redis & Formatter
        # --------------------------------------------------
        self.redis_client = RedisClient()
        self.formatter = MessageFormatter(
            tpoms_name="ZERODHA",
            entity_id=self.entity_id
        )

        # --------------------------------------------------
        # Order caches
        # --------------------------------------------------
        self.blitz_to_zerodha: Dict[str, str] = {}
        self.zerodha_to_blitz: Dict[str, str] = {}
        self.order_request_data: Dict[str, Dict] = {}

        # --------------------------------------------------
        # AUTO LOGIN
        # --------------------------------------------------
        self.logger.info("LOGIN_START | Starting Zerodha login")

        self.access_token, error = get_access_token(
            api_key=self.api_key,
            api_secret=self.api_secret,
            user_id=self.user_id,
            password=self.password,
            totp_secret=self.totp_secret,
        )

        if not self.access_token:
            self.logger.error(f"LOGIN_FAILED | {error}")
            raise RuntimeError(f"Zerodha login failed: {error}")

        self.logger.info(f"LOGIN_SUCCESS | Access token received: {self.access_token}")

        # --------------------------------------------------
        # APIs
        # --------------------------------------------------
        self.order_api = ZerodhaOrderAPI(
            api_key=self.api_key,
            access_token=self.access_token
        )

        # --------------------------------------------------
        # Instruments
        # --------------------------------------------------
        try:
            self.instruments_api = ZerodhaInstruments(logger=self.logger)
            self.instruments_api.download_instruments()
            self.instruments_api.load_instruments()
            self.logger.info("INSTRUMENTS_READY | Instruments loaded")
        except Exception as e:
            self.logger.warning(f"INSTRUMENTS_FAILED | {e}")
            self.instruments_api = None

        # --------------------------------------------------
        # WebSocket (TPOMS controlled)
        # --------------------------------------------------
        self.ws: Optional[ZerodhaWebSocket] = None

        self.logger.info("Adapter READY | Zerodha adapter initialized")

    # ============================================================
    # WebSocket (called by TPOMS)
    # ============================================================
    def _start_websocket(self):
        if self.ws:
            self.logger.info("WS_RESTART | Stopping old WebSocket")
            self.ws.stop()
            self.ws = None

        self.logger.info("WS_START | Starting Zerodha WebSocket")

        self.ws = ZerodhaWebSocket(
            api_key=self.api_key,
            access_token=self.access_token,
            user_id=self.user_id,
            redis_client=self.redis_client,
            formatter=self.formatter,
            request_data_mapping=self.order_request_data,
            logger=self.logger,
        )

        self.ws.start()

    def stop(self):
        if self.ws:
            self.ws.stop()
            self.ws = None  # Ensure clean state for reconnect
        self.logger.info("Adapter STOP | Zerodha adapter stopped")

    # ============================================================
    # Command processing
    # ============================================================
    def process_command(self, payload: Dict[str, Any]):
        action = payload.get("Action")
        blitz_data = payload.get("Data")

        self.logger.info(
            "[BLITZ-INBOUND] | action=%s payload=%s",
            action,
            blitz_data
        )

        if action == "PLACE_ORDER":
            self._handle_place_order(blitz_data)

        elif action == "MODIFY_ORDER":
            self._handle_modify_order(blitz_data)

        elif action == "CANCEL_ORDER":
            self._handle_cancel_order(blitz_data)

        else:
            self.logger.warning(f"COMMAND_UNSUPPORTED | action={action}")

    # ============================================================
    # PLACE ORDER
    # ============================================================
    def _handle_place_order(self, blitz_data: Dict[str, Any]):
        blitz_id = blitz_data.get("BlitzAppOrderID")

        # --------------------------------------------------
        # TEMP CACHE
        # --------------------------------------------------
        if blitz_id:
            temp_key = f"blitz_{blitz_id}"
            self.order_request_data[temp_key] = blitz_data
            self.logger.info(
                "CACHE_TEMP | blitz_id=%s",
                blitz_id
            )

        try:
            params = ZerodhaMapper.to_zerodha(
                blitz_data,
                instruments_api=self.instruments_api
            )

            # Zerodha Rest request
            self.logger.info(
                "[TPOMS-OUTBOUND] | payload=%s",
                params
            )

            response = self.order_api.place_order(**params)

            # Zerodha Rest response
            self.logger.info(
                "[TPOMS-INBOUND] | payload=%s",
                response
            )

            if response.get("status") == "error":
                order_log = ZerodhaMapper.error_to_orderlog(
                    response.get("message"),
                    blitz_data,
                    self.entity_id
                )
                
                # Log Blitz response before publishing
                self.logger.info(
                    "[BLITZ-OUTBOUND] | payload=%s",
                    order_log.to_dict() if hasattr(order_log, 'to_dict') else order_log
                )
                
                self._publish_order_update(order_log)
                return

            order_id = response["data"]["order_id"]

            self.blitz_to_zerodha[str(blitz_id)] = str(order_id)
            self.zerodha_to_blitz[str(order_id)] = str(blitz_id)

            self.order_request_data[str(order_id)] = blitz_data
            self.order_request_data.pop(f"blitz_{blitz_id}", None)

            # ===================================================
            # >>> ADDED: replay early WS update (race fix)
            # ===================================================
            if self.ws and hasattr(self.ws, "pending_ws_updates"):
                with self.ws.pending_lock:
                    pending = self.ws.pending_ws_updates.pop(str(order_id), None)
                    if pending:
                        self.logger.info(
                            "[WS_REPLAY] | order_id=%s",
                            order_id
                        )
                        # Call _on_order_update outside the lock to avoid deadlock
                if pending:
                    self.ws._on_order_update(None, pending)
            # ===================================================

            self.logger.info(
                "ORDER_PLACED | blitz=%s zerodha=%s",
                blitz_id,
                order_id
            )

        except Exception as e:
            self.logger.error(
                f"PLACE_FAILED | {e}",
                exc_info=True
            )

            order_log = ZerodhaMapper.error_to_orderlog(
                str(e),
                blitz_data,
                self.entity_id
            )
            self._publish_order_update(order_log)

    # ============================================================
    # MODIFY ORDER
    # ============================================================
    def _handle_modify_order(self, blitz_data: Dict[str, Any]):
        original_request = None
        try:
            zerodha_order_id = blitz_data.get("ExchangeOrderID")
            if not zerodha_order_id:
                raise RuntimeError("Missing ExchangeOrderID")

            original_request = self.order_request_data.get(str(zerodha_order_id))
            if not original_request:
                raise RuntimeError(
                    f"No cached request for order {zerodha_order_id}"
                )

            modify_params = {}

            if blitz_data.get("ModifiedOrderQuantity") is not None:
                modify_params["quantity"] = int(
                    blitz_data["ModifiedOrderQuantity"]
                )

            if blitz_data.get("ModifiedLimitPrice") is not None:
                modify_params["price"] = float(
                    blitz_data["ModifiedLimitPrice"]
                )

            if blitz_data.get("ModifiedStopPrice") is not None:
                modify_params["trigger_price"] = float(
                    blitz_data["ModifiedStopPrice"]
                )

            if blitz_data.get("ModifiedTimeInForce"):
                modify_params["validity"] = ZerodhaMapper._map_validity(
                    blitz_data["ModifiedTimeInForce"]
                )

            if blitz_data.get("ModifiedOrderType"):
                modify_params["order_type"] = ZerodhaMapper._map_order_type(
                    blitz_data["ModifiedOrderType"]
                )

            if blitz_data.get("ModifiedDisclosedQuantity") is not None:
                modify_params["disclosed_quantity"] = int(
                    blitz_data["ModifiedDisclosedQuantity"]
                )

            if not modify_params:
                raise ValueError("No modifiable fields")

            # Zerodha Modify request
            self.logger.info(
                "[TPOMS-OUTBOUND] | order_id=%s payload=%s",
                zerodha_order_id,
                modify_params
            )

            response = self.order_api.modify_order(
                order_id=zerodha_order_id,
                **modify_params
            )

            # Zerodha Modify response
            self.logger.info(
                "[TPOMS-INBOUND] | order_id=%s payload=%s",
                zerodha_order_id,
                response
            )

            if isinstance(response, dict) and response.get("status") == "error":
                order_log = ZerodhaMapper.error_to_orderlog_modify_rejected(
                    response.get("message"),
                    original_request,
                    self.entity_id
                )
                self._publish_order_update(order_log)
                return

            original_request.update(modify_params)
            self.order_request_data[str(zerodha_order_id)] = original_request

            self.logger.info(
                "[ORDER_MODIFIED] | blitz=%s zerodha=%s",
                original_request.get("BlitzAppOrderID"),
                zerodha_order_id
            )

        except Exception as e:
            self.logger.error(
                f"MODIFY_FAILED | {e}",
                exc_info=True
            )

            order_log = ZerodhaMapper.error_to_orderlog_modify_rejected(
                str(e),
                original_request,
                self.entity_id
            )
            self._publish_order_update(order_log)

    # ============================================================
    # CANCEL ORDER
    # ============================================================
    def _handle_cancel_order(self, blitz_data: Dict[str, Any]):
        try:
            zerodha_order_id = self.blitz_to_zerodha.get(
                str(blitz_data.get("BlitzAppOrderID"))
            )

            if not zerodha_order_id:
                raise RuntimeError("Order ID mapping not found")

            original_request = self.order_request_data.get(str(zerodha_order_id))

            # Zerodha Cancel request
            self.logger.info(
                "[TPOMS-OUTBOUND] | order_id=%s",
                zerodha_order_id
            )

            response = self.order_api.cancel_order(zerodha_order_id)

            # Zerodha Cancel response
            self.logger.info(
                "[TPOMS-INBOUND] | order_id=%s payload=%s",
                zerodha_order_id,
                response
            )

            if response.get("status") == "error":
                order_log = ZerodhaMapper.error_to_orderlog_cancel_rejected(
                    response.get("message"),
                    original_request,
                    self.entity_id
                )
                self._publish_order_update(order_log)
                return

            # SUCCESS: Map cancel success to Blitz format
            zerodha_cancel_data = {
                "order_id": zerodha_order_id,
                "status": "CANCELLED"
            }
            
            order_log = ZerodhaMapper.to_blitz_orderlog(
                zerodha_data=zerodha_cancel_data,
                blitz_request=original_request
            )
            
            # Explicitly set OrderStatus to Cancelled
            order_log.OrderStatus = "Cancelled"
            
            # Log Blitz response before publishing
            self.logger.info(
                "[BLITZ-OUTBOUND] | payload=%s",
                order_log.to_dict() if hasattr(order_log, 'to_dict') else order_log
            )
            
            self._publish_order_update(order_log)

            self.logger.info(
                "[ORDER_CANCELLED] | blitz=%s zerodha=%s",
                blitz_data.get("BlitzAppOrderID"),
                zerodha_order_id
            )

        except Exception as e:
            self.logger.error(
                f"CANCEL_FAILED | {e}",
                exc_info=True
            )

            order_log = ZerodhaMapper.error_to_orderlog_cancel_rejected(
                str(e),
                self.order_request_data.get(
                    self.blitz_to_zerodha.get(
                        str(blitz_data.get("BlitzAppOrderID"))
                    )
                ),
                self.entity_id
            )
            self._publish_order_update(order_log)

    # ============================================================
    # Publish (TPOMS-compatible)
    # ============================================================
    def _publish_order_update(self, order_log):
        message = self.formatter.order_update(order_log)
        self.redis_client.publish(message.get("Data"))

    @property
    def websocket(self):
        return self.ws
