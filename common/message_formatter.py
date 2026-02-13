class MessageFormatter:
    def __init__(self, tpoms_name, entity_id=None, user_id=None):
        self.tpoms_name = tpoms_name
        self.entity_id = entity_id
        self.user_id = user_id or entity_id
    
    def connection_status(self, status, message, user_id=None):
        return {
            "MessageType": "TPOMSConnectionStatus",
            "TPOmsName": self.tpoms_name,
            "UserId": user_id or self.user_id,
            "Data": {"status": status, "message": message}
        }
    
    def system_event(self, status, entity_id=None, **kwargs):
        data = {"status": status}
        data.update(kwargs)
        return {
            "MessageType": "TPOMSSystemEvent",
            "TPOmsName": self.tpoms_name,
            "UserId": entity_id or self.entity_id,
            "Data": data
        }
    
    def order_update(self, order_log, entity_id=None, message_type=None):
        """Blitz-format order update. message_type: request Action (e.g. PLACE_ORDER) to correlate response."""
        order_data = order_log.to_dict() if hasattr(order_log, 'to_dict') else order_log
        return {
            "MessageType": message_type or "TPOMSOrderUpdate",
            "TPOmsName": self.tpoms_name,
            "UserId": entity_id or self.entity_id,
            "Data": order_data
        }

    def orders(self, orders_data, entity_id=None, message_type=None):
        """Blitz-format orders response. message_type: request Action (e.g. PLACE_ORDER) so response maps to request."""
        data = []
        for order in orders_data:
            if hasattr(order, 'to_dict'):
                data.append(order.to_dict())
            else:
                data.append(order)
        return {
            "MessageType": message_type or "TPOMSOrders",
            "TPOmsName": self.tpoms_name,
            "UserId": entity_id or self.entity_id,
            "Data": data
        }

    def positions(self, positions_data, entity_id=None, message_type=None):
        """Blitz-format positions. message_type: request Action (e.g. GET_POSITIONS)."""
        return {
            "MessageType": message_type or "TPOMSPositions",
            "TPOmsName": self.tpoms_name,
            "UserId": entity_id or self.entity_id,
            "Data": positions_data
        }

    def holdings(self, holdings_data, entity_id=None, message_type=None):
        """Blitz-format holdings. message_type: request Action (e.g. GET_HOLDINGS)."""
        return {
            "MessageType": message_type or "TPOMSHoldings",
            "TPOmsName": self.tpoms_name,
            "UserId": entity_id or self.entity_id,
            "Data": holdings_data
        }

    def trades(self, trades_data, entity_id=None, message_type=None):
        """Blitz-format trades. message_type: request Action (e.g. GET_TRADES)."""
        return {
            "MessageType": message_type or "TPOMSTrades",
            "TPOmsName": self.tpoms_name,
            "UserId": entity_id or self.entity_id,
            "Data": trades_data if isinstance(trades_data, list) else (trades_data if trades_data else [])
        }

    def response(self, message_type, status, message,  user_id=None):
        return {
            "MessageType": message_type,
            "TPOmsName": self.tpoms_name,
            "UserId": user_id or self.user_id or self.entity_id,
            "Data": {"status": status, "message": message}
        }


def format_tpoms_connection_status(tpoms_name, user_id, status, message):
    formatter = MessageFormatter(tpoms_name, user_id=user_id)
    return formatter.connection_status(status, message)


def format_tpoms_system_event(tpoms_name, entity_id, status, **kwargs):
    formatter = MessageFormatter(tpoms_name, entity_id=entity_id)
    return formatter.system_event(status, **kwargs)


def format_tpoms_order_update(tpoms_name, entity_id, order_log):
    formatter = MessageFormatter(tpoms_name, entity_id=entity_id)
    return formatter.order_update(order_log)


def format_tpoms_orders(tpoms_name, entity_id, orders_data):
    formatter = MessageFormatter(tpoms_name, entity_id=entity_id)
    return formatter.orders(orders_data)


def format_tpoms_positions(tpoms_name, entity_id, positions_data):
    formatter = MessageFormatter(tpoms_name, entity_id=entity_id)
    return formatter.positions(positions_data)


def format_tpoms_holdings(tpoms_name, entity_id, holdings_data):
    formatter = MessageFormatter(tpoms_name, entity_id=entity_id)
    return formatter.holdings(holdings_data)


def format_tpoms_response(message_type, tpoms_name, entity_id, status, message):
    formatter = MessageFormatter(tpoms_name, entity_id=entity_id)
    return formatter.response(message_type, status, message)

