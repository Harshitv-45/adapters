import json
import requests
import logging

logger = logging.getLogger("MotilalOrderAPI")


class MotilalOswalOrderAPI:
    BASE_URL = "https://openapi.motilaloswal.com"

    def __init__(self, api_key, client_code, jwt_token, api_secret_key=None, access_token=None):
        self.api_key = api_key
        self.client_code = client_code
        self.jwt_token = jwt_token
        self.api_secret_key = api_secret_key
        self.access_token = access_token or jwt_token

    def _headers(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "MOSL/V.1.1.0",
            "ApiKey": self.api_key,
            "vendorinfo": self.client_code,
            "ClientLocalIp": "127.0.0.1",
            "ClientPublicIp": "127.0.0.1",
            "MacAddress": "00:11:22:33:44:55",
            "SourceId": "WEB",
            "osname": "Windows",
            "osversion": "10",
            "devicemodel": "PC",
            "manufacturer": "Generic",
            "productname": "Algo",
            "productversion": "1.0",
            "browsername": "Chrome",
            "browserversion": "120"
        }
        
        if self.jwt_token:
            headers["Authorization"] = self.jwt_token
        
        if self.access_token:
            headers["accesstoken"] = self.access_token
        elif self.jwt_token:
            headers["accesstoken"] = self.jwt_token
        
        if self.api_secret_key:
            headers["apisecretkey"] = self.api_secret_key
        
        return headers

    def place_order(
        self,
        symboltoken,
        exchange,
        side,
        quantity,
        amoorder,
        order_type,
        product_type,
        price=0,
        trigger_price=0,
        validity="DAY",
        disclosedquantity=0,
        goodtilldate=None,
        algoid="",
        tag="",
        participantcode=""
    ):
        url = f"{self.BASE_URL}/rest/trans/v1/placeorder"

        # Use symboltoken directly
        if symboltoken is None:
            raise ValueError("symboltoken is required")
        
        # Convert to int if not already an integer
        if not isinstance(symboltoken, int):
            try:
                symboltoken = int(symboltoken)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid symboltoken type: {type(symboltoken)}, must be int or convertible to int")
        
        logger.info(f"Using ExchangeInstrumentID as symbol token: {symboltoken}")
        
        payload = {
            "exchange": str(exchange).upper(),
            "symboltoken": symboltoken,
            "buyorsell": str(side).upper(),
            "ordertype": str(order_type).upper() if order_type else None,
            "producttype": str(product_type).upper(),
            "quantityinlot": int(quantity),
            "amoorder": str(amoorder).upper(),
            "price": float(price),
            "triggerprice": float(trigger_price),
            "orderduration": str(validity).upper(),
        }
        
        if disclosedquantity > 0:
            payload["disclosedquantity"] = int(disclosedquantity)
        if goodtilldate:
            payload["goodtilldate"] = str(goodtilldate)
        if algoid:
            payload["algoid"] = str(algoid)
        if tag:
            payload["tag"] = str(tag)[:10]
        if participantcode:
            payload["participantcode"] = str(participantcode)

        logger.info(f"[API REQUEST] {json.dumps(payload)}")
        
        res = requests.post(
            url,
            headers=self._headers(),
            json=payload
        )
        
        # Handle empty response
        if not res.text or not res.text.strip():
            logger.warning(f"Empty response from place_order API. Status: {res.status_code}")
            return {"status": "ERROR", "message": "Empty response from API", "errorcode": "EMPTY_RESPONSE"}
        
        try:
            json_response = res.json()
        except ValueError as e:
            logger.error(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
            raise Exception(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
        
        # Check HTTP status code
        if res.status_code not in [200, 201]:
            error_msg = json_response.get("message", f"HTTP {res.status_code} error")
            error_code = json_response.get("errorcode", f"HTTP_{res.status_code}")
            logger.error(f"Place order failed: {error_msg} (code: {error_code})")
            raise Exception(f"API Error [{error_code}]: {error_msg}")
        
        # Motilal returns status as "SUCCESS" string (uppercase)
        status = json_response.get("status", "").upper()
        if status == "SUCCESS":
            order_id = json_response.get("uniqueorderid") or json_response.get("orderid")
            logger.info(f"Order placed successfully: {order_id}")
            return json_response
        elif status == "ERROR":
            error_msg = json_response.get("message", "Unknown error")
            error_code = json_response.get("errorcode", "")
            logger.error(f"Place order API error: {error_msg} (code: {error_code})")
            raise Exception(f"API Error [{error_code}]: {error_msg}")
        else:
            # Unknown status, return as-is but log warning
            logger.warning(f"Unknown status in place_order response: {status}, response: {json_response}")
            return json_response

    def modify_order(
        self,
        order_id,
        order_type,
        validity,
        price,
        quantity,
        prev_timestamp,
        traded_quantity,
        disclosedquantity=0
    ):
        url = f"{self.BASE_URL}/rest/trans/v2/modifyorder"

        payload = {
            "uniqueorderid": order_id,
            "newordertype": order_type,
            "neworderduration": validity,
            "newprice": price,
            "newquantityinlot": quantity,
            "lastmodifiedtime": prev_timestamp,
            "qtytradedtoday": traded_quantity,
        }
        
        # Only add disclosedquantity if > 0
        if disclosedquantity > 0:
            payload["disclosedquantity"] = int(disclosedquantity)

        logger.info(f"[API REQUEST] {json.dumps(payload)}")

        res = requests.post(
            url,
            headers=self._headers(),
            json=payload
        )
        
        # Handle empty response
        if not res.text or not res.text.strip():
            logger.warning(f"Empty response from modify_order API. Status: {res.status_code}")
            return {"status": "ERROR", "message": "Empty response from API", "errorcode": "EMPTY_RESPONSE"}
        
        try:
            json_response = res.json()
        except ValueError as e:
            logger.error(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
            raise Exception(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
        
        # Check HTTP status code
        if res.status_code not in [200, 201]:
            error_msg = json_response.get("message", f"HTTP {res.status_code} error")
            error_code = json_response.get("errorcode", f"HTTP_{res.status_code}")
            logger.error(f"Modify order failed: {error_msg} (code: {error_code})")
            raise Exception(f"API Error [{error_code}]: {error_msg}")
        
        # Motilal returns status as "SUCCESS" string (uppercase)
        status = json_response.get("status", "").upper()
        if status == "SUCCESS":
            order_id = json_response.get("uniqueorderid") or json_response.get("orderid")
            logger.info(f"Order modified successfully: {order_id}")
            return json_response
        elif status == "ERROR":
            error_msg = json_response.get("message", "Unknown error")
            error_code = json_response.get("errorcode", "")
            logger.error(f"Modify order API error: {error_msg} (code: {error_code})")
            raise Exception(f"API Error [{error_code}]: {error_msg}")
        else:
            # Unknown status, return as-is but log warning
            logger.warning(f"Unknown status in modify_order response: {status}, response: {json_response}")
            return json_response

    def cancel_order(self, order_id):
        url = f"{self.BASE_URL}/rest/trans/v1/cancelorder"

        payload = {
            "uniqueorderid": order_id
        }

        logger.info(f"[API REQUEST] {json.dumps(payload)}")

        res = requests.post(
            url,
            headers=self._headers(),
            json=payload
        )
        
        # Handle empty response
        if not res.text or not res.text.strip():
            logger.warning(f"Empty response from cancel_order API. Status: {res.status_code}")
            return {"status": "ERROR", "message": "Empty response from API", "errorcode": "EMPTY_RESPONSE"}
        
        try:
            json_response = res.json()
        except ValueError as e:
            logger.error(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
            raise Exception(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
        
        # Motilal returns status as "SUCCESS" string (uppercase)
        status = json_response.get("status", "").upper()
        if status == "SUCCESS":
            logger.info(f"Order cancelled successfully: {order_id}")
            return {"status": "success", "orderid": order_id}
        elif status == "ERROR":
            error_msg = json_response.get("message", "Failed to cancel order")
            error_code = json_response.get("errorcode", "")
            logger.error(f"Cancel order API error: {error_msg} (code: {error_code})")
            raise Exception(f"API Error [{error_code}]: {error_msg}")
        else:
            # Unknown status, return error
            logger.warning(f"Unknown status in cancel_order response: {status}, response: {json_response}")
            return {"status": "error", "message": json_response.get("message", "Failed to cancel order")}

    def get_orders(self):
        url = f"{self.BASE_URL}/rest/book/v2/getorderbook"

        payload = {
            "clientcode": self.client_code
        }

        logger.info(f"[API REQUEST] {json.dumps(payload)}")

        headers = self._headers()
        
        res = requests.post(
            url,
            headers=headers,
            json=payload,
            allow_redirects=False
        )

        if res.status_code in [301, 302, 303, 307, 308]:
            raise Exception(f"API endpoint redirected. Status: {res.status_code}, Location: {res.headers.get('Location')}")

        res.raise_for_status()

        if not res.text or not res.text.strip():
            return {"status": "success", "data": []}

        content_type = res.headers.get('content-type', '').lower()
        response_text = res.text.strip()

        if 'html' in content_type or response_text.startswith('<') or response_text.startswith('<!'):
            raise Exception(f"API returned HTML instead of JSON. Check endpoint URL: {url}. Response: {response_text[:200]}")

        if not response_text.startswith('{') and not response_text.startswith('['):
            raise Exception(f"API returned non-JSON response. URL: {url}, Response: {response_text[:200]}")

        try:
            json_response = res.json()
            
            # Motilal returns status as "SUCCESS" string (uppercase)
            if isinstance(json_response, dict):
                status = json_response.get("status", "").upper()
                if status == "SUCCESS":
                    # Handle case where data might be None
                    data = json_response.get('data')
                    if data is None:
                        data = []
                    order_count = len(data) if isinstance(data, list) else 0
                    logger.info(f"Successfully fetched order book with {order_count} orders")
                    return json_response
                elif status == "ERROR":
                    error_msg = json_response.get("message", "Unknown error")
                    error_code = json_response.get("errorcode", "")
                    logger.error(f"Get orders API error: {error_msg} (code: {error_code})")
                    raise Exception(f"API Error [{error_code}]: {error_msg}")
            
            return json_response
        except ValueError as e:
            raise Exception(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {response_text[:200]}")

    def get_order_by_id(self, order_id):
        return self.get_order_history(order_id)

    def get_order_history(self, order_id):
        url = f"{self.BASE_URL}/rest/trans/v1/orderhistory"

        payload = {
            "clientcode": self.client_code,
            "orderid": order_id
        }

        logger.info(f"[API REQUEST] {json.dumps(payload)}")

        res = requests.post(
            url,
            headers=self._headers(),
            json=payload
        )
        
        # Handle empty response
        if not res.text or not res.text.strip():
            logger.warning(f"Empty response from get_order_history API. Status: {res.status_code}")
            return {"status": "ERROR", "message": "Empty response from API", "errorcode": "EMPTY_RESPONSE"}
        
        try:
            json_response = res.json()
        except ValueError as e:
            logger.error(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
            raise Exception(f"Invalid JSON response: {e}, Status: {res.status_code}, Response: {res.text[:200]}")
        
        # Motilal returns status as "SUCCESS" string (uppercase)
        status = json_response.get("status", "").upper()
        if status == "SUCCESS":
            logger.info(f"Successfully fetched order history for order_id: {order_id}")
            return json_response
        elif status == "ERROR":
            error_msg = json_response.get("message", "Unknown error")
            error_code = json_response.get("errorcode", "")
            logger.error(f"Get order history API error: {error_msg} (code: {error_code})")
            raise Exception(f"API Error [{error_code}]: {error_msg}")
        else:
            # Unknown status, return as-is but log warning
            logger.warning(f"Unknown status in get_order_history response: {status}, response: {json_response}")
            return json_response

