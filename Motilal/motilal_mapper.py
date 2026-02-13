from common.broker_order_mapper import OrderLog
# from Motilal.motilal_adapter import MotilalAdapter


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
    def map_exchange_segment(seg):
        if not seg:
            return None

        seg = str(seg).upper()

        segment_map = {
            "NSE": "NSECM",
            "NSEFO": "NSEFO",
            "NSECM":"NSE"

        }

        return segment_map.get(seg, seg)


    PRODUCT_TYPE_MAP = {
        "MIS": "NORMAL",
        "CNC": "DELIVERY",
        "NORMAL": "MIS",
        "DELIVERY":"CNC"

    }

    @staticmethod
    def map_producttype(value):
        """
        Generic product type mapper (works both ways)
        """
        if not value:
            return None

        value = str(value)
        return MotilalMapper.PRODUCT_TYPE_MAP.get(value, value)

    @staticmethod
    def map_status(status, action=None):
        if not status:
            return None

        status = str(status).strip().upper()
        action = str(action).strip().upper()

        # -------- ACTION-INDEPENDENT --------
        if status == "TRADED":
            return "Filled"

        if status == "PARTIAL":
            return "PartiallyFilled"

        if status == "CANCEL":
            return "Cancelled"

        # -------- PLACE ORDER --------
        if action == "PLACE_ORDER":
            return {
                "CONFIRM": "New",
                "REJECTED": "Rejected",
                "ERROR": "Rejected"
            }.get(status)

        # -------- MODIFY ORDER --------
        if action == "MODIFY_ORDER":
            return {
                "CONFIRM": "Replaced",
                "REJECTED": "ReplaceRejected",
                "ERROR": "ReplaceRejected"
            }.get(status)


        # -------- CANCEL ORDER --------
        if action == "CANCEL_ORDER":
            return {
                "CONFIRM": "Cancelled",
                "REJECTED": "CancelRejected",
                "ERROR": "CancelRejected"
            }.get(status)

        return None

    @staticmethod
    def map_tif_orderlog(validity):
        """
        Map Motilal validity string (DAY, GTC, IOC, GTD, etc.)
        to Blitz TimeInForce (GFD, GTC, IOC, GTD, etc.)
        """
        if not validity:
            return None

        reverse_map = {
            "DAY": "GFD",   
            "GTC": "GTC",    
            "IOC": "IOC",    
            "GTD": "GTD",   
        }

        return reverse_map.get(str(validity).upper(), str(validity).upper())

    @staticmethod
    def map_tif(tif):
        """
        Map Blitz TimeInForce (numeric or string) to Motilal validity string
        """
        if not tif:
            return None

        string_map = {
            "GFD": "DAY",   # Good For Day → DAY
            "GTC": "GTC",   # Good Till Cancel → GTC
            "IOC": "IOC",   # Immediate Or Cancel → IOC
            "FOK": "IOC",   # Fill Or Kill → IOC
            "GTD": "GTD",   # Good Till Date → GTD
            "COL": "DAY",   # COL → DAY (default)
            "DAY": "DAY",   # Already in Motilal format
        }

        return string_map.get(str(tif).upper())

    def map_ordertype(type):
        if not type:
            return None
        map_type = {
            "LIMIT": "LIMIT",
            "MARKET": "MARKET",
            "STOPLIMIT": "STOPLOSS",
            "STOPLOSS":"STOPLIMIT"
        }
        return map_type.get(str(type).upper())
    @staticmethod
    def to_motilal(data):
        exchange_instrument_id = data.get("ExchangeInstrumentID")

        exchange_seg = data.get("ExchangeSegment")
        exchange = MotilalMapper.map_exchange_segment(exchange_seg)
        tag = data.get("BlitzAppOrderID")
        side = (data.get("OrderSide"))

        # order_type = (data.get("OrderType")).upper()

        order_type = MotilalMapper.map_ordertype(data.get("OrderType"))
        
        quantity = int(data.get("OrderQuantity") or 0)
        
        if exchange == "NSEFO":
            lot_size = 65  
            quantity = quantity // lot_size

        product_type = MotilalMapper.map_producttype(data.get("ProductType"))

        order_type_upper = order_type if order_type else ""
        if order_type_upper == "MARKET":
            price = 0.0
        else:
            price = float(data.get("LimitPrice"))

        trigger_price = float(data.get("StopPrice") or 0.0)
        blitz_time_in_force = data.get("TimeInForce")
        
        validity = MotilalMapper.map_tif(blitz_time_in_force)
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
            "tag": tag,
            "amoorder": "N"
        }

        disclosed_qty = int(data.get("DisclosedQuantity"))
        if disclosed_qty > 0:
            payload["disclosedquantity"] = disclosed_qty

        required_fields = ["symboltoken", "exchange", "side", "order_type", "quantity",
                          "product_type", "price", "trigger_price", "validity","tag", "amoorder"]
        if "disclosedquantity" in payload:
            required_fields.append("disclosedquantity")

        return MotilalMapper._filter_payload(payload, required_fields)


    @staticmethod
    def to_motilal_modify(data,cashed_data, order_id):
        """Map Blitz OrderModification to Motilal MODIFY request"""

        # ---- Mandatory ----
        uniqueorderid = order_id

        newordertype = MotilalMapper.map_ordertype(data.get("ModifiedOrderType"))
        neworderduration = MotilalMapper.map_tif(data.get("ModifiedTimeInForce"))

        newquantityinlot = int(data.get("ModifiedOrderQuantity") )

        exchange = cashed_data.get("ExchangeSegment")
        if exchange == "NSEFO":
            lot_size = 65  
            newquantityinlot = newquantityinlot // lot_size

        traded_quantity = int(data.get("CummulativeQuantity") or 0)

        lastmodifiedtime = (cashed_data.get("LastModifiedDateTime"))
                    #         if isinstance(cashed_data, dict)
                    # else getattr(cashed_data, "LastModifiedDateTime", None))


        payload = {
            "uniqueorderid": uniqueorderid,
            "newordertype": newordertype,
            "neworderduration": neworderduration,
            "newquantityinlot": newquantityinlot,
            "qtytradedtoday": traded_quantity,
            "lastmodifiedtime": lastmodifiedtime
        }

        # ---- Price handling ----
        if newordertype == "MARKET":
            payload["newprice"] = 0
            payload["newtriggerprice"] = 0
        else:
            payload["newprice"] = float(data.get("ModifiedLimitPrice") or 0)
            payload["newtriggerprice"] = float(data.get("ModifiedStopPrice") or 0)

        # ---- Optional fields ----
        disclosed_qty = int(data.get("ModifiedDisclosedQuantity") or 0)

        clientcode = data.get("Account")
        if clientcode: payload["clientcode"] = clientcode

        return payload


    @staticmethod
    def error_to_orderlog(error_msg, blitz_data=None, err_status=None, action=None):
        """
        Build OrderLog for API errors.
        """
        order_log = OrderLog()
        order_log.CancelRejectReason = error_msg

        merged_data = blitz_data.copy() if blitz_data else {}

        # Map status using MotilalMapper
        order_log.OrderStatus = MotilalMapper.map_status(err_status, action)

        if merged_data:
            order_log.BlitzAppOrderID = str(merged_data.get("BlitzAppOrderID") or "")
            order_log.ExchangeInstrumentID = int(merged_data.get("ExchangeInstrumentID") or 0)
            order_log.ExchangeOrderID = str(merged_data.get("ExchangeOrderID") or "0")
            order_log.ExchangeSegment = merged_data.get("ExchangeSegment") or ""
            order_log.OrderType = merged_data.get("OrderType") or merged_data.get("ModifiedOrderType") or ""
            order_log.OrderSide = merged_data.get("OrderSide") or ""
            order_log.ProductType = merged_data.get("ProductType") or merged_data.get("ModifiedProductType") or ""
            order_log.OrderQuantity = int(merged_data.get("OrderQuantity") or merged_data.get("ModifiedOrderQuantity") or 0)
            order_log.OrderPrice = float(merged_data.get("LimitPrice") or merged_data.get("ModifiedLimitPrice") or 0.0)
            order_log.OrderStopPrice = float(merged_data.get("StopPrice") or merged_data.get("ModifiedStopPrice") or 0.0)
            order_log.TimeInForce = merged_data.get("TimeInForce") or merged_data.get("ModifiedTimeInForce") or ""
            order_log.OrderDisclosedQuantity = int(merged_data.get("DisclosedQuantity") or merged_data.get("ModifiedDisclosedQuantity") or 0)
            order_log.Account = merged_data.get("Account") or ""
            order_log.ExchangeClientID = merged_data.get("ExchangeClientID") or ""

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


    @staticmethod
    def map_order(data,o,  cashed_data, action):

        o.ExchangeInstrumentID = int(data.get("symboltoken") or 0)
        # Prefer request’s ExchangeSegment (e.g. NSECM) over API’s exchange (e.g. NSE)
        o.ExchangeSegment = MotilalMapper.map_exchange_segment(data.get("exchange"))
        blitz_id = (cashed_data.get("BlitzAppOrderID") if isinstance(cashed_data, dict)
            else getattr(cashed_data, "BlitzAppOrderID", None))

        o.BlitzAppOrderID = blitz_id

        o.ExchangeOrderID = data.get("orderid")

        o.ExecutionID = data.get("executionid")
        
        # o.OrderType = data.get("ordertype", "").upper()
        o.OrderType = MotilalMapper.map_ordertype(data.get("ordertype"))
        _side = data.get("buyorsell", "")
        o.OrderSide = _side.capitalize()

        o.ProductType = MotilalMapper.map_producttype(data.get("producttype"))

        o.OrderStatus = MotilalMapper.map_status(data.get("orderstatus"), action)

        order_qty = int(data.get("orderqty", 0))
        # if o.ExchangeSegment == "NSEFO":
        #     lot_size = 65
        #     order_qty = order_qty * lot_size
        #     o.OrderQuantity = order_qty

        o.OrderQuantity = order_qty
        o.LeavesQuantity = int(data.get("totalqtyremaining", 0))
        o.LastTradedQuantity = int(data.get("qtytradedtoday", 0))
        o.CumulativeQuantity = int(data.get("qtytradedtoday", 0))
        # o.OrderPrice= data.get("price") or 0.
        price = data.get("price")
        

        o.OrderAverageTradedPrice = (data.get("averageprice")or 0.)

        o.TimeInForce = MotilalMapper.map_tif_orderlog(data.get("orderduration"))
        o.OrderDisclosedQuantity = int(data.get("disclosedqty", 0))

        o.OrderGeneratedDateTime = data.get("entrydatetime")
        o.ExchangeTransactTime = data.get("entrydatetime")
        o.LastUpdateDateTime = data.get("lastmodifiedtime")
        o.LastExecutionTransactTime = data.get("lastmodifiedtime")

        avg_price = data.get("averageprice")
        
        o.LastTradedPrice  = avg_price / 100
       

        o.OrderStopPrice = data.get("triggerprice") or 0.0
        o.CancelRejectReason = data.get("error")
        o.Account = data.get("clientid")
        exchangeclientid = (cashed_data.get("ExchangeClientID") if isinstance(cashed_data, dict)
                    else getattr(cashed_data, "ExchangeClientID", None))

        o.ExchangeClientID = exchangeclientid
        
        o.LastUpdateDateTime = data.get("lastmodifiedtime")
        

    @staticmethod
    def extract_order_id(result):
        """
        Robustly extracts Motilal order_id from various response formats.
        """
        if isinstance(result, dict):
            # Check if order_id is nested in 'data' field
            if "Data" in result and isinstance(result["Data"], dict):
                return result["Data"].get("uniqueorderid") or result["Data"].get("orderid")
            else:
                return result.get("uniqueorderid") or result.get("orderid")
        else:
            return result


    @staticmethod
    def resolve_order_id(data=None, id_mapping=None, *, direction="BLITZ_TO_MOTILAL", order_id=None):
        """
        Resolve order IDs in both directions.
        """

        if direction == "BLITZ_TO_MOTILAL":
            if not isinstance(data, dict):
                raise ValueError("data must be a dict containing 'BlitzAppOrderID'")

            blitz_order_id = data.get("BlitzAppOrderID")
            if not blitz_order_id:
                raise ValueError("Missing mandatory field: 'BlitzAppOrderID'")

            motilal_order_id = id_mapping.get(str(blitz_order_id))
            if motilal_order_id:
                print(
                    f"Resolved BlitzAppOrderID={blitz_order_id} -> uniqueorderid={motilal_order_id}"
                )
                return motilal_order_id

            raise ValueError(f"Blitz order ID '{blitz_order_id}' not found in mapping")

        elif direction == "MOTILAL_TO_BLITZ":
            if not order_id:
                raise ValueError("order_id (uniqueorderid) is required")

            blitz_id = id_mapping.get(str(order_id))
            if blitz_id:
                print(
                    f"Resolved uniqueorderid={order_id} -> BlitzAppOrderID={blitz_id}"
                )
                return blitz_id

            print(f"No Blitz mapping found for uniqueorderid={order_id}")
            return None

        else:
            raise ValueError(f"Invalid direction: {direction}")



