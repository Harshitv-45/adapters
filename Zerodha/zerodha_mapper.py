"""
Zerodha Mapper (Production – FIXED)

Responsibilities:
- Blitz → Zerodha payload (PLACE / MODIFY)
- Zerodha → Blitz OrderLog
- Error → Rejected / ReplaceRejected / CancelRejected
- Resolve Blitz ↔ Zerodha order IDs
"""

from typing import Dict, Optional
from common.broker_order_mapper import OrderLog


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
            o.OrderQuantity = int(blitz_request.get("OrderQuantity") or 0)
            o.OrderPrice = float(blitz_request.get("LimitPrice") or 0)
            o.OrderStopPrice = float(blitz_request.get("StopPrice") or 0)
            o.OrderDisclosedQuantity = int(blitz_request.get("DisclosedQuantity") or 0)

        # Zerodha fields
        o.ExchangeOrderID = zerodha_data.get("order_id", "")
        o.OrderStatus = ZerodhaMapper._map_status(zerodha_data.get("status"))
        o.CumulativeQuantity = int(zerodha_data.get("filled_quantity") or 0)
        o.LeavesQuantity = int(zerodha_data.get("pending_quantity") or 0)
        o.OrderAverageTradedPrice = zerodha_data.get("average_price") or 0
        o.LastUpdateDateTime = zerodha_data.get("exchange_update_timestamp", "")
        o.ExchangeTransactTime = zerodha_data.get("exchange_timestamp", "")
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

        }.get(str(status).upper(), str(status))
