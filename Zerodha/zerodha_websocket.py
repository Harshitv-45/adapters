from kiteconnect import KiteTicker
from Zerodha.zerodha_mapper import ZerodhaMapper


class ZerodhaWebSocket:
    def __init__(
        self,
        api_key: str,
        access_token: str,
        user_id: str,
        redis_client,
        formatter,
        request_data_mapping: dict,
        logger,
    ):
        self.api_key = api_key
        self.access_token = access_token
        self.user_id = user_id

        self.redis_client = redis_client
        self.formatter = formatter
        self.request_data_mapping = request_data_mapping
        self.logger = logger

        self.kws = None
        self.is_connected = False

        # order_id -> last known state
        self.order_state_cache = {}

        # >>> ADDED: cache WS updates that arrive before REST mapping
        self.pending_ws_updates = {}

    # ---------------------------------------------------------
    def start(self):
        ws_token = f"{self.access_token}&user_id={self.user_id}"

        self.kws = KiteTicker(self.api_key, ws_token)
        self.kws.on_connect = self._on_connect
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_order_update = self._on_order_update

        self.kws.connect(threaded=True)

    def stop(self):
        if self.kws:
            self.kws.close()

    # ---------------------------------------------------------
    def _on_connect(self, ws, response):
        self.is_connected = True
        self.logger.info("[WS] Connected to Zerodha")

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        self.logger.warning(f"[WS] Closed: {code} {reason}")

    def _on_error(self, ws, code, reason):
        self.logger.error(f"[WS] Error: {code} {reason}")

    # ---------------------------------------------------------
    def _on_order_update(self, ws, data):
        try:
            # =====================================================
            # RAW ZERODHA WS PAYLOAD
            # =====================================================
            self.logger.info(
                "ZERODHA_WS_RESPONSE | payload=%s",
                data
            )

            order_id = data.get("order_id")
            if not order_id:
                return

            order_id = str(order_id)
            status = (data.get("status") or "").upper()

            blitz_request = self.request_data_mapping.get(order_id)

            # >>> CHANGED: cache WS update instead of dropping it
            if not blitz_request:
                self.logger.warning(
                    "ZERODHA_WS_PENDING | order_id=%s payload=%s",
                    order_id,
                    data
                )
                self.pending_ws_updates[order_id] = data
                return

            prev = self.order_state_cache.get(order_id)

            price = data.get("price")
            qty = data.get("quantity")
            pending_qty = int(data.get("pending_quantity") or 0)

            # =====================================================
            # IGNORE UPDATE (exchange noise)
            # =====================================================
            if status == "UPDATE":
                self.order_state_cache[order_id] = {
                    **(prev or {}),
                    "last_update_seen": True
                }
                return

            # =====================================================
            # MODIFY CONFIRMATION → ONE Replaced
            # =====================================================
            if (
                status == "OPEN"
                and prev
                and prev.get("status") == "OPEN"
                and (
                    price != prev.get("price")
                    or qty != prev.get("quantity")
                )
            ):
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )
                order_log.OrderStatus = "Replaced"

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "OPEN",
                    "price": price,
                    "quantity": qty,
                }

                self.logger.info(
                    f"[WS] MODIFY published | order_id={order_id}"
                )
                return

            # =====================================================
            # CANCEL → ignore intermediate
            # =====================================================
            if status == "CANCELLED" and pending_qty > 0:
                return

            # =====================================================
            # FINAL CANCEL
            # =====================================================
            if status == "CANCELLED":
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )
                order_log.OrderStatus = "Cancelled"
                order_log.LeavesQuantity = 0

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "CANCELLED"
                }

                self.logger.info(
                    f"[WS] CANCEL published | order_id={order_id}"
                )
                return

            # =====================================================
            # COMPLETE (Filled)
            # =====================================================
            if status == "COMPLETE":
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "COMPLETE"
                }
                return

            # =====================================================
            # FIRST OPEN → New Order
            # =====================================================
            if status == "OPEN" and not prev:
                order_log = ZerodhaMapper.to_blitz_orderlog(
                    zerodha_data=data,
                    blitz_request=blitz_request
                )

                self.logger.info(
                    "BLITZ_RESPONSE | order_id=%s payload=%s",
                    order_id,
                    order_log.__dict__
                )

                self.redis_client.publish(
                    self.formatter.order_update(order_log)
                )

                self.order_state_cache[order_id] = {
                    "status": "OPEN",
                    "price": price,
                    "quantity": qty,
                }

                self.logger.info(
                    f"[WS] NEW order published | order_id={order_id}"
                )
                return

        except Exception as e:
            self.logger.error(
                f"[WS] Failed to process order update: {e}",
                exc_info=True
            )
