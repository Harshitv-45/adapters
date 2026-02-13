import logging
from common.broker_order_mapper import OrderLog

logger = logging.getLogger(__name__)

class MotilalMapper:

    @staticmethod
    def _filter_payload(payload, required_fields):
        """Filter payload to include only required fields, ignore extra fields"""
        filtered = {}
        for field in required_fields:
            if field in payload:
                filtered[field] = payload[field]
        return filtered

    @staticmethod
    def to_motilal(data):
        # ExchangeInstrumentID mapping - use as symbol token
        exchange_instrument_id = data.get("ExchangeInstrumentID") or data.get("ExchangeInstrumentId") or data.get("exchangeInstrumentID")
        if exchange_instrument_id is None:
            raise ValueError("ExchangeInstrumentID is required in the request payload")
        
        # Exchange mapping - handle Blitz field names
        exchange_seg = (data.get("ExchangeSegment"))
        exchange = exchange_seg[:3] if exchange_seg else "NSE"
        
        # Side mapping - handle Blitz field names
        side = (data.get("OrderSide"))
        
        # Order type mapping - handle Blitz field names
        order_type = (data.get("OrderType"))
        
        # Quantity mapping - handle Blitz field names
        quantity = int(data.get("OrderQuantity") or 0)
        
        # Product type mapping - handle Blitz field names
        product_type = (data.get("ProductType"))
        
        # Price mapping - handle Blitz field names (LimitPrice, OrderPrice)
        # Ignore price if order type is MARKET
        order_type_upper = order_type.upper() if order_type else ""
        if order_type_upper == "MARKET":
            price = 0.0
        else:
            price = float(data.get("LimitPrice") or data.get("OrderPrice") or data.get("price") or 0.0)
        
        # Trigger price mapping - handle Blitz field names
        trigger_price = float(data.get("StopPrice") or 0.0)
        
        # Validity/TimeInForce mapping - handle Blitz field names
        validity = (data.get("TimeInForce"))
        
        # Only include required fields for API - ignore any extra fields
        payload = {
            "symboltoken": exchange_instrument_id,
            "exchange": exchange,
            "side": side,
            "order_type": order_type,
            "quantity": quantity,
            "product_type": product_type,
            "price": price,
            "trigger_price": trigger_price,
            "validity": validity,
            "amoorder": "N"
        }
        
        # Add optional field only if provided and > 0
        # Don't include disclosedquantity if it's 0 or not provided
        disclosed_qty = int(data.get("DisclosedQuantity") or data.get("disclosedquantity") or data.get("OrderDisclosedQuantity") or 0)
        if disclosed_qty > 0:
            payload["disclosedquantity"] = disclosed_qty
        
        # Filter to include only required fields, ignore any extra fields
        required_fields = ["symboltoken", "exchange", "side", "order_type", "quantity", 
                          "product_type", "price", "trigger_price", "validity", "amoorder"]
        if "disclosedquantity" in payload:
            required_fields.append("disclosedquantity")
        
        return MotilalMapper._filter_payload(payload, required_fields)

    @staticmethod
    def to_motilal_modify(data):
        """Map MODIFY_ORDER request with Modified* fields"""
        # Extract modified fields (with Modified prefix) or fallback to regular fields
        order_type = (data.get("ModifiedOrderType") or data.get("OrderType") or "")
        quantity = int(data.get("ModifiedOrderQuantity") or data.get("OrderQuantity") or 0)
        product_type = (data.get("ModifiedProductType") or data.get("ProductType") or "")
        
        # Price mapping - handle ModifiedLimitPrice, LimitPrice, OrderPrice
        order_type_upper = order_type.upper() if order_type else ""
        if order_type_upper == "MARKET":
            price = 0.0
        else:
            price = float(data.get("ModifiedLimitPrice") or data.get("LimitPrice") or data.get("OrderPrice") or data.get("price") or 0.0)
        
        # Trigger price mapping - handle ModifiedStopPrice, StopPrice
        trigger_price = float(data.get("ModifiedStopPrice") or data.get("StopPrice") or 0.0)
        
        # Validity/TimeInForce mapping - handle ModifiedTimeInForce, TimeInForce
        validity = (data.get("ModifiedTimeInForce") or data.get("TimeInForce") or "")
        
        # Note: prev_timestamp (lastmodifiedtime) is NOT extracted from request
        # It must come from websocket cache only
        
        # Only include required fields for API - ignore any extra fields
        payload = {
            "order_type": order_type,
            "quantity": quantity,
            "product_type": product_type,
            "price": price,
            "trigger_price": trigger_price,
            "validity": validity
        }
        
        # Add optional field only if provided and > 0
        disclosed_qty = int(data.get("ModifiedDisclosedQuantity") or data.get("DisclosedQuantity") or 
                           data.get("OrderDisclosedQuantity") or data.get("disclosedquantity") or 0)
        if disclosed_qty > 0:
            payload["disclosedquantity"] = disclosed_qty
        
        # Filter to include only required fields, ignore any extra fields
        required_fields = ["order_type", "quantity", "product_type", "price", "trigger_price", "validity"]
        if "disclosedquantity" in payload:
            required_fields.append("disclosedquantity")
        
        return MotilalMapper._filter_payload(payload, required_fields)

    @staticmethod
    def to_blitz(raw_data, request_type, entity_id=None, action_name=None):
        try:
            mapped_data = []
            data = raw_data

            if isinstance(data, dict) and "Data" in data:
                data = data["Data"]
                if data is None:
                    data = []

            # Determine data type from request_type or action_name
            data_type = request_type.lower()
            if action_name:
                # Map action names to data types
                action_to_type = {
                    "PLACE_ORDER": "orders",
                    "MODIFY_ORDER": "orders",
                    "CANCEL_ORDER": "orders",
                    "GET_ORDERS": "orders",
                    "GET_ORDER_DETAILS": "orders",
                    "GET_POSITIONS": "positions",
                    "GET_HOLDINGS": "holdings"
                }
                data_type = action_to_type.get(action_name, data_type)

            if data_type == "orders":
                if isinstance(data, list):
                    for item in data:
                        log_obj = OrderLog()
                        MotilalMapper._map_order(item, log_obj, entity_id=entity_id)
                        mapped_data.append(log_obj)
                elif isinstance(data, dict):
                    log_obj = OrderLog()
                    MotilalMapper._map_order(data, log_obj, entity_id=entity_id)
                    mapped_data.append(log_obj)
                elif data is None:
                    mapped_data = []

            elif data_type == "positions":
                if isinstance(data, list):
                    for p in data:
                        mapped_data.append(MotilalMapper._map_position(p))
                else:
                    mapped_data = []

            elif data_type == "holdings":
                if isinstance(data, list):
                    for h in data:
                        mapped_data.append(MotilalMapper._map_holding(h))
            else:
                 mapped_data = data

            # Use action_name if provided, otherwise format request_type
            if action_name:
                message_type = action_name
            else:
                message_type_pascal = request_type[0].upper() + request_type[1:].lower() if request_type else request_type
                if message_type_pascal == "orders":
                    message_type_pascal = "Orders"
                elif message_type_pascal == "positions":
                    message_type_pascal = "Positions"
                elif message_type_pascal == "holdings":
                    message_type_pascal = "Holdings"
                message_type = message_type_pascal
            
            blitz_response = {
                "MessageType": message_type,
                "TPOmsName": "MOFL",
                "UserId": entity_id,
                "Data": mapped_data
            }
            
            return blitz_response
        except Exception as e:
            logger.error(f"Failed to standardize {request_type}: {e}")
            return None

    @staticmethod
    def error_to_orderlog(error_msg, blitz_data=None, entity_id=None):
        order_log = OrderLog()
        order_log.EntityId = entity_id or ""
        order_log.OrderStatus = "REJECTED"
        order_log.UserText = error_msg
        order_log.ExecutionType = "Rejected"
        order_log.IsOrderCompleted = True
        
        # Handle array of data dictionaries
        if isinstance(blitz_data, list):
            # Merge all dictionaries in the array
            merged_data = {}
            for data_item in blitz_data:
                if isinstance(data_item, dict):
                    merged_data.update(data_item)
            blitz_data = merged_data if merged_data else None
        
        if blitz_data:
            order_log.BlitzOrderId = blitz_data.get("BlitzAppOrderID") or blitz_data.get("BlitzAppOrderId")
            # Handle both request format and Blitz field names
            order_log.InstrumentId = int(blitz_data.get("ExchangeInstrumentID") or 
                                       blitz_data.get("ExchangeInstrumentId") or 
                                       blitz_data.get("InstrumentId") or 
                                       blitz_data.get("instrumentId") or 0)
            order_log.InstrumentName = (blitz_data.get("SymbolName") or 
                                       blitz_data.get("ExchangeInstrumentName") or
                                       blitz_data.get("InstrumentName") or 
                                       blitz_data.get("TradingSymbol") or 
                                       blitz_data.get("Symbol") or "")
            order_log.ExchangeSegment = (blitz_data.get("ExchangeSegment") or 
                                        blitz_data.get("Exchange") or "")
            order_log.OrderType = (blitz_data.get("OrderType") or 
                                  blitz_data.get("orderType") or "")
            order_log.OrderSide = (blitz_data.get("OrderSide") or 
                                  blitz_data.get("orderSide") or "")
            order_log.OrderQuantity = int(blitz_data.get("OrderQuantity") or 
                                         blitz_data.get("quantity") or 0)
            # Handle LimitPrice, OrderPrice, price
            order_log.OrderPrice = float(blitz_data.get("LimitPrice") or 
                                        blitz_data.get("OrderPrice") or 
                                        blitz_data.get("price") or 0.0)
            order_log.OrderTriggerPrice = float(blitz_data.get("StopPrice") or 
                                               blitz_data.get("trigger_price") or 0.0)
            order_log.TIF = (blitz_data.get("TimeInForce") or 
                            blitz_data.get("validity") or "")
            # Handle DisclosedQuantity, OrderDisclosedQuantity, disclosedquantity
            order_log.OrderDisclosedQuantity = int(blitz_data.get("DisclosedQuantity") or 
                                                   blitz_data.get("OrderDisclosedQuantity") or 
                                                   blitz_data.get("disclosedquantity") or 0)
        
        return order_log

    @staticmethod
    def _map_order(data, o, entity_id=None):
        # Use websocket order data directly - it contains all order details
        details = data.get("details", data) if isinstance(data, dict) else {}

        o.Id = 0
        o.EntityId = entity_id or ""

        # Instrument mapping - use websocket order data
        o.InstrumentId = int(details.get("symboltoken") or details.get("SymbolToken") or details.get("instrumentid") or 0)
        o.ExchangeSegment = details.get("exchange") or details.get("Exchange") or details.get("exchangesegment") or ""
        
        o.InstrumentName = details.get("symbol") or details.get("Symbol") or details.get("instrumentname") or ""

        # Order identifiers
        o.BlitzOrderId = None
        o.ExchangeOrderId = details.get("uniqueorderid", details.get("orderid"))

        o.ExecutionId = 0

        # Order type & side
        o.OrderType = details.get("ordertype", "").upper()
        o.OrderSide = details.get("buyorsell", "").upper()

        #  STATUS MAPPING 
        raw_status = details.get("orderstatus", "").upper()
        o.OrderStatus = MotilalMapper._map_status(raw_status)

        #  QUANTITY MAPPING 
        o.OrderQuantity = int(details.get("orderqty", 0))
        o.LeavesQuantity = int(details.get("totalqtyremaining", 0))
        o.LastTradedQuantity = int(details.get("qtytradedtoday", 0))

        # Prices
        o.OrderPrice = float(details.get("price", 0.0))
        o.OrderStopPrice = 0.0
        o.OrderTriggerPrice = float(details.get("triggerprice", 0.0))
        
        # Average price (needed for trade messages)
        avg_price = float(details.get("averageprice", 0.0))
        o.AverageTradedPrice = avg_price
        
        # For trade messages (FILLED/PARTIALLYFILLED), ensure LastTradedQuantity and LastTradedPrice are NEVER 0
        if o.OrderStatus in ["FILLED", "PARTIALLYFILLED"]:
            # Calculate LastTradedQuantity - must not be 0 for trade messages
            if o.LastTradedQuantity == 0:
                total_traded = int(details.get("totalqtytraded", 0))
                if total_traded > 0:
                    o.LastTradedQuantity = total_traded
                elif o.OrderQuantity > 0:
                    calculated_qty = o.OrderQuantity - o.LeavesQuantity
                    if calculated_qty > 0:
                        o.LastTradedQuantity = calculated_qty
                    else:
                        # For FILLED orders, if calculation fails, use full order quantity
                        o.LastTradedQuantity = o.OrderQuantity if o.OrderStatus == "FILLED" else max(1, o.OrderQuantity)
            
            # Ensure LastTradedQuantity is never 0 for trade messages
            if o.LastTradedQuantity == 0 and o.OrderQuantity > 0:
                o.LastTradedQuantity = o.OrderQuantity
            
            # Set LastTradedPrice - must not be 0 for trade messages
            if avg_price > 0.0:
                o.LastTradedPrice = avg_price
            elif o.OrderPrice > 0.0:
                o.LastTradedPrice = o.OrderPrice
            else:
                # Fallback: use AverageTradedPrice if available
                if o.AverageTradedPrice > 0.0:
                    o.LastTradedPrice = o.AverageTradedPrice
                else:
                    # Last resort: cannot be 0 for trade messages
                    logger.warning(f"LastTradedPrice is 0 for trade message. OrderId: {o.ExchangeOrderId}, Status: {o.OrderStatus}")
                    o.LastTradedPrice = o.OrderPrice if o.OrderPrice > 0.0 else 0.0
        else:
            o.LastTradedPrice = 0.0

        # Time in force
        o.TIF = details.get("orderduration", "")

        # Disclosed quantity
        o.OrderDisclosedQuantity = int(details.get("disclosedqty", 0))

        # Exchange time
        o.ExchangeTransactTime = details.get("recordinserttime", "")

        # IsOrderCompleted: true only for REJECTED, FILLED, CANCELLED
        o.IsOrderCompleted = o.OrderStatus in ["REJECTED", "FILLED", "CANCELLED", "CANCELED"]

        #  ERROR / USER MESSAGE
        o.UserText = details.get("error", "")

        #  EXECUTION TYPE 
        if o.OrderStatus == "REJECTED":
            o.ExecutionType = "Rejected"
        elif o.OrderStatus == "FILLED":
            o.ExecutionType = "Trade"
        elif o.OrderStatus == "PARTIALLYFILLED":
            o.ExecutionType = "PartialFill"
        elif o.OrderStatus == "CANCELLED" or o.OrderStatus == "CANCELED":
            o.ExecutionType = "Cancelled"
        elif o.OrderStatus == "REPLACED":
            o.ExecutionType = "Replace"
        elif o.OrderStatus == "MODIFIED":
            o.ExecutionType = "Modified"
        elif o.OrderStatus == "REPLACEREJECTED":
            o.ExecutionType = "Rejected"
        elif o.OrderStatus == "CANCELREJECTED":
            o.ExecutionType = "Rejected"
        elif o.OrderStatus == "NEW":
            o.ExecutionType = "New"
        elif o.OrderStatus == "PENDINGNEW":
            o.ExecutionType = "PendingNew"
        elif o.OrderStatus == "PENDINGCANCEL":
            o.ExecutionType = "PendingCancel"
        elif o.OrderStatus == "PENDINGREPLACE":
            o.ExecutionType = "PendingReplace"
        elif o.OrderStatus == "EXPIRED":
            o.ExecutionType = "Expired"
        else:
            o.ExecutionType = "None"

        o.CorrelationOrderId = 0

    @staticmethod
    def _map_position(p):
        return {
            "TradingSymbol": p.get("tradingsymbol", p.get("symbol", "")),
            "Exchange": p.get("exchange", ""),
            "InstrumentToken": p.get("instrumenttoken", p.get("symboltoken", 0)),
            "Product": p.get("producttype", p.get("product", "")),
            "Quantity": p.get("quantity", 0),
            "OvernightQuantity": p.get("overnightquantity", 0),
            "Multiplier": p.get("multiplier", 1),
            "AveragePrice": p.get("averageprice", p.get("average_price", 0.0)),
            "LastPrice": p.get("lastprice", p.get("last_price", 0.0)),
            "Value": p.get("value", 0.0),
            "PnL": p.get("pnl", p.get("unrealisedpnl", 0.0)),
            "M2M": p.get("m2m", 0.0),
            "Unrealised": p.get("unrealised", 0.0),
            "Realised": p.get("realised", 0.0),
            "BuyQuantity": p.get("buyquantity", 0),
            "BuyPrice": p.get("buyprice", 0.0),
            "BuyValue": p.get("buyvalue", 0.0),
            "SellQuantity": p.get("sellquantity", 0),
            "SellPrice": p.get("sellprice", 0.0),
            "SellValue": p.get("sellvalue", 0.0),
            "DayBuyQuantity": p.get("daybuyquantity", 0),
            "DayBuyPrice": p.get("daybuyprice", 0.0),
            "DayBuyValue": p.get("daybuyvalue", 0.0),
            "DaySellQuantity": p.get("daysellquantity", 0),
            "DaySellPrice": p.get("daysellprice", 0.0),
            "DaySellValue": p.get("daysellvalue", 0.0)
        }

    @staticmethod
    def _map_holding(h):
        return {
            "TradingSymbol": h.get("tradingsymbol", h.get("symbol", "")),
            "Exchange": h.get("exchange", ""),
            "InstrumentToken": h.get("instrumenttoken", h.get("symboltoken", 0)),
            "ISIN": h.get("isin", ""),
            "Product": h.get("producttype", h.get("product", "")),
            "Price": h.get("price", 0.0),
            "Quantity": h.get("quantity", 0),
            "T1Quantity": h.get("t1quantity", 0),
            "RealisedQuantity": h.get("realisedquantity", 0),
            "CollateralQuantity": h.get("collateralquantity", 0),
            "CollateralType": h.get("collateraltype", ""),
            "AveragePrice": h.get("averageprice", h.get("average_price", 0.0)),
            "LastPrice": h.get("lastprice", h.get("last_price", 0.0)),
            "PnL": h.get("pnl", 0.0),
            "DayChange": h.get("daychange", 0.0),
            "DayChangePercentage": h.get("daychangepercentage", 0.0)
        }

    @staticmethod
    # def _map_status(status: str) -> str:
    #     mapping = {
    #         "OPEN": "NEW",
    #         "COMPLETE": "FILLED",
    #         "FILLED": "FILLED",
    #         "CANCELLED": "CANCELLED",
    #         "REJECTED": "REJECTED",
    #         "PENDING": "NEW",
    #     }
    #     return mapping.get(status, status)
    @staticmethod
    def _map_status(status):
        status_upper = status.upper() if status else ""
        
        status_map = {
            "UNKNOWN": "UNKNOWN",
            "SENT": "PENDINGNEW",
            "CONFIRM": "NEW",
            "CANCEL": "CANCELED",
            "PARTIAL": "PARTIALLYFILLED",
            "TRADED": "FILLED",
            "REJECTED": "REJECTED",
            "ERROR": "REJECTED",
            "REPLACED": "REPLACED",
            "REPLACEREJECTED": "REPLACEREJECTED",
            "CANCELREJECTED": "CANCELREJECTED",
            "MODIFIED": "MODIFIED",
        }
        
        return status_map.get(status_upper, "UNKNOWN")

    @staticmethod
    def extract_order_id(result):
        """
        Robustly extracts Motilal order_id from various response formats.
        """
        if isinstance(result, dict):
            # Check if order_id is nested in 'data' field
            if "Data" in result and isinstance(result["Data"], dict):
                return result["Data"].get("uniqueorderid") or result["Data"].get("order_id") or result["Data"].get("orderid")
            else:
                return result.get("uniqueorderid") or result.get("order_id") or result.get("orderid")
        else:
            return result

    @staticmethod
    def resolve_order_id(data, id_mapping):
        """
        Resolves Motilal uniqueorderid from BlitzAppOrderID using the provided mapping.
        BlitzAppOrderID is mandatory.
        Returns uniqueorderid to use when calling Motilal API.
        """
        blitz_order_id = data.get("BlitzAppOrderID")
        
        if not blitz_order_id:
             raise ValueError("Missing mandatory field: 'BlitzAppOrderID'")

        motilal_order_id = id_mapping.get(blitz_order_id)
        if motilal_order_id:
            logger.info(f"Resolved blitz_order_id={blitz_order_id} -> uniqueorderid={motilal_order_id}")
            return motilal_order_id
        
        raise ValueError(f"Blitz order ID '{blitz_order_id}' not found in mapping")

