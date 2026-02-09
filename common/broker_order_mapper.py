import json
from typing import Optional, Any

class OrderLog:
    """
    Standardized Order Log format for Blitz, serialized to match 
    C# MergedOrderTradeReportJsonSerialization JSON structure.
    """
    def __init__(self):
        self.SequenceNumber: int = 0
        self.Account: str = ""
        self.ExchangeClientID: Optional[str] = None
        self.BlitzAppOrderID: str = ""
        self.ExchangeOrderID: Optional[str] = None
        self.ExchangeSegment: str = ""
        self.ExchangeInstrumentID: int = 0
        self.OrderSide: str = ""
        self.OrderType: str = ""
        self.ProductType: Optional[str] = None
        self.TimeInForce: str = ""
        self.OrderPrice: float = 0.0
        self.OrderQuantity: int = 0
        self.OrderStopPrice: float = 0.0
        self.OrderStatus: str = ""
        self.OrderAverageTradedPrice: Optional[str] = None
        self.LeavesQuantity: int = 0
        self.CumulativeQuantity: int = 0
        self.OrderDisclosedQuantity: int = 0
        self.OrderGeneratedDateTime: Optional[str] = None
        self.ExchangeTransactTime: Optional[str] = None
        self.LastUpdateDateTime: Optional[str] = None
        self.CancelRejectReason: Optional[str] = None
        self.LastTradedPrice: float = 0.0
        self.LastTradedQuantity: int = 0
        self.LastExecutionTransactTime: Optional[str] = None
        self.ExecutionID: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert OrderLog to dictionary matching C# JSON structure. No nulls in output: optional fields emit \"\" or 0."""
        def _str(v):
            return v if v is not None else ""
        def _int(v):
            return v if v is not None else 0
        def _float(v):
            return v if v is not None else 0.0
        return {
            "SequenceNumber": _int(self.SequenceNumber),
            "Account": _str(self.Account),
            "ExchangeClientID": _str(self.ExchangeClientID),
            "BlitzAppOrderID": _str(self.BlitzAppOrderID),
            "ExchangeOrderID": _str(self.ExchangeOrderID),
            "ExchangeSegment": _str(self.ExchangeSegment),
            "ExchangeInstrumentID": _int(self.ExchangeInstrumentID),
            "OrderSide": _str(self.OrderSide),
            "OrderType": _str(self.OrderType),
            "ProductType": _str(self.ProductType),
            "TimeInForce": _str(self.TimeInForce),
            "OrderPrice": _float(self.OrderPrice),
            "OrderQuantity": _int(self.OrderQuantity),
            "OrderStopPrice": _float(self.OrderStopPrice),
            "OrderStatus": _str(self.OrderStatus),
            "OrderAverageTradedPrice": _str(self.OrderAverageTradedPrice),
            "LeavesQuantity": _int(self.LeavesQuantity),
            "CumulativeQuantity": _int(self.CumulativeQuantity),
            "OrderDisclosedQuantity": _int(self.OrderDisclosedQuantity),
            "OrderGeneratedDateTime": _str(self.OrderGeneratedDateTime),
            "ExchangeTransactTime": _str(self.ExchangeTransactTime),
            "LastUpdateDateTime": _str(self.LastUpdateDateTime),
            "CancelRejectReason": _str(self.CancelRejectReason),
            "LastTradedPrice": _float(self.LastTradedPrice),
            "LastTradedQuantity": _int(self.LastTradedQuantity),
            "LastExecutionTransactTime": _str(self.LastExecutionTransactTime),
            "ExecutionID": _str(self.ExecutionID)
        }

    def to_json(self) -> str:
        """Convert OrderLog to JSON string matching C# JSON structure."""
        return json.dumps(self.to_dict(), default=str)
    
    @staticmethod
    def orderlog_error(error_msg, blitz_data=None, err_status=None, action=None):
        """
        Build OrderLog for API errors. 
        """
        order_log = OrderLog()
        order_log.CancelRejectReason = error_msg

        merged_data = blitz_data.copy() if isinstance(blitz_data, dict) else {}

        # Map status using MotilalMapper
        order_log.OrderStatus = "Rejected"

        if merged_data:
            order_log.BlitzAppOrderID = str(merged_data.get("BlitzAppOrderID") or "")
            order_log.ExchangeInstrumentID = int(merged_data.get("ExchangeInstrumentID") or 0)
            order_log.ExchangeSegment = merged_data.get("ExchangeSegment") or ""
            order_log.OrderType = merged_data.get("OrderType") or ""
            order_log.OrderSide = merged_data.get("OrderSide") or ""
            order_log.ProductType = merged_data.get("ProductType/KeyInfo1")
            order_log.OrderQuantity = int(merged_data.get("OrderQuantity") or 0)
            order_log.OrderPrice = float(merged_data.get("LimitPrice") or 0.0)
            order_log.OrderStopPrice = float(
                merged_data.get("StopPrice") or merged_data.get("trigger_price") or 0.0
            )
            order_log.TimeInForce = merged_data.get("TimeInForce") or ""
            order_log.OrderDisclosedQuantity = int(merged_data.get("DisclosedQuantity") or 0)
            order_log.Account = merged_data.get("Account") or ""
            order_log.ExchangeClientID = merged_data.get("ExchangeClientID")

            # Helper to normalize date fields
            _invalid_date = "01-Jan-1980 00:00:00"
            def _clean_date(v):
                v = (v or "").strip()
                return "" if not v or v == _invalid_date else v

            entry_dt = _clean_date(merged_data.get("EntryDateTime"))
            last_dt = _clean_date(merged_data.get("LastModifiedTime"))

            order_log.OrderGeneratedDateTime = entry_dt or _clean_date(merged_data.get("OrderGeneratedDateTime"))
            order_log.ExchangeTransactTime = last_dt or entry_dt or _clean_date(merged_data.get("ExchangeTransactTime"))
            order_log.LastUpdateDateTime = last_dt or _clean_date(merged_data.get("LastUpdateDateTime"))

        return order_log


    
