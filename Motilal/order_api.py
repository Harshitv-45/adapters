import json
import requests

class MotilalOswalOrderAPI:
    BASE_URL = "https://openapi.motilaloswal.com"

    def __init__(self, api_key, client_code, jwt_token, api_secret_key=None, access_token=None, logger=None):
        self.api_key = api_key
        self.client_code = client_code
        self.jwt_token = jwt_token
        self.api_secret_key = api_secret_key
        self.access_token = access_token or jwt_token
        self.logger = logger

    # ------------------------------------------------------------------
    # Headers
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Place Order
    # ------------------------------------------------------------------
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

        if symboltoken is None:
            raise ValueError("symboltoken is required")

        if not isinstance(symboltoken, int):
            try:
                symboltoken = int(symboltoken)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid symboltoken type: {type(symboltoken)}")

        self.logger.info(f"Using ExchangeInstrumentID as symbol token: {symboltoken}")

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
            "tag": tag
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

        self.logger.info(f"[API REQUEST] {json.dumps(payload)}")

        res = requests.post(url, headers=self._headers(), json=payload)

        if not res.text or not res.text.strip():
            return {"status": "ERROR", "message": "Empty response from API", "errorcode": "EMPTY_RESPONSE"}

        try:
            json_response = res.json()
        except ValueError as e:
            raise Exception(f"Invalid JSON response: {e}, Status: {res.status_code}")

        if res.status_code not in [200, 201]:
            raise Exception(json.dumps(json_response))

        status = json_response.get("status", "").upper()

        if status == "SUCCESS":
            self.logger.info(f"Order placed successfully")
            return json_response

        if status == "ERROR":
            # raise Exception(f"API Error [{json_response.get('status')}]: {json_response.get('message')}")
            raise Exception(json.dumps(json_response))
        
        return json_response

    # ------------------------------------------------------------------
    # Modify Order
    # ------------------------------------------------------------------
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

        if disclosedquantity > 0:
            payload["disclosedquantity"] = int(disclosedquantity)

        self.logger.info(f"[API REQUEST] {json.dumps(payload)}")

        res = requests.post(url, headers=self._headers(), json=payload)

        if not res.text or not res.text.strip():
            return {"status": "ERROR", "message": "Empty response", "errorcode": "EMPTY_RESPONSE"}

        json_response = res.json()
        status = json_response.get("status", "").upper()

        if status == "SUCCESS":
            return json_response
        
        if status == "ERROR":
                    raise Exception(json.dumps(json_response))
        
        return json_response
    # ------------------------------------------------------------------
    # Cancel Order
    # ------------------------------------------------------------------
    def cancel_order(self, order_id):
        url = f"{self.BASE_URL}/rest/trans/v1/cancelorder"

        payload = {"uniqueorderid": order_id}

        self.logger.info(f"[API REQUEST] {json.dumps(payload)}")

        res = requests.post(url, headers=self._headers(), json=payload)

        if not res.text or not res.text.strip():
            return {"status": "ERROR", "message": "Empty response", "errorcode": "EMPTY_RESPONSE"}

        json_response = res.json()
        status = json_response.get("status", "").upper()

        if status == "SUCCESS":
            return {"status": "success", "orderid": order_id}

        if status == "ERROR":
                    raise Exception(json.dumps(json_response))
        
        return json_response
    
    
    # ------------------------------------------------------------------
    # Trade Book
    # ------------------------------------------------------------------
    def get_tradebook(self):
        url = f"{self.BASE_URL}/rest/book/v1/gettradebook"
        payload = {"clientcode": self.client_code}

        res = requests.post(url, headers=self._headers(), json=payload)
        res.raise_for_status()

        if not res.text or not res.text.strip():
            return {"status": "SUCCESS", "data": []}

        json_response = res.json()
        status = json_response.get("status", "").upper()
        
        if json_response.get("data") is None:
                json_response["data"] = []

        if status == "SUCCESS":
            return json_response

        raise Exception(json.dumps(json_response))
    
    # ------------------------------------------------------------------
    # Order Book
    # ------------------------------------------------------------------
    def get_orders(self):
        url = f"{self.BASE_URL}/rest/book/v2/getorderbook"
        payload = {"clientcode": self.client_code}

        res = requests.post(url, headers=self._headers(), json=payload)
        res.raise_for_status()

        if not res.text or not res.text.strip():
            return {"status": "success", "data": []}

        json_response = res.json()
        status = json_response.get("status", "").upper()

        if status == "SUCCESS":
            return json_response

        raise Exception(json.dumps(json_response))
    
    def get_order_by_unique_id(self, unique_order_id):
        url = f"{self.BASE_URL}/rest/book/v2/getorderdetailbyuniqueorderid"
        payload = {"uniqueorderid": unique_order_id}

        res = requests.post(url, headers=self._headers(), json=payload)
        res.raise_for_status()

        if not res.text or not res.text.strip():
            return {"status": "success", "data": {}}

        json_response = res.json()
        status = json_response.get("status", "").upper()

        if status == "SUCCESS":
            return json_response

        raise Exception(json.dumps(json_response, indent=2))


    # ------------------------------------------------------------------
    # Order History
    # ------------------------------------------------------------------
    def get_order_by_id(self, order_id):
        return self.get_order_history(order_id)

    def get_order_history(self, order_id):
        url = f"{self.BASE_URL}/rest/trans/v1/orderhistory"

        payload = {
            "clientcode": self.client_code,
            "orderid": order_id
        }

        res = requests.post(url, headers=self._headers(), json=payload)

        if not res.text or not res.text.strip():
            return {"status": "ERROR", "message": "Empty response"}

        json_response = res.json()
        status = json_response.get("status", "").upper()

        if status == "SUCCESS":
            return json_response

        raise Exception(json.dumps(json_response))
    
     # ==================================================================
    # PORTFOLIO APIs
    # ==================================================================

    
    def get_holdings(self):
        url = f"{self.BASE_URL}/rest/report/v1/getholdings"
        payload = {"clientcode": self.client_code}

        res = requests.post(url, headers=self._headers(), json=payload)
        return res.json()

    def get_positions(self):
        url = f"{self.BASE_URL}/rest/report/v1/getpositions"
        payload = {"clientcode": self.client_code}

        res = requests.post(url, headers=self._headers(), json=payload)
        return res.json()
