"""
Zerodha Order API Module (Simple & Direct)
"""

import requests


class ZerodhaOrderAPI:
    BASE_URL = "https://api.kite.trade"

    def __init__(self, access_token: str, api_key: str) -> None:
        self.headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{access_token}",
        }

    # Place Order
    def place_order(
        self,
        tradingsymbol,
        exchange,
        transaction_type,
        order_type,
        quantity,
        product,
        price,
        trigger_price,
        disclosed_quantity,
        validity,
    ):
        url = f"{self.BASE_URL}/orders/regular"

        payload = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": transaction_type,
            "order_type": order_type,
            "quantity": quantity,
            "product": product,
            "price": price,
            "trigger_price": trigger_price,
            "disclosed_quantity": disclosed_quantity,
            "validity": validity,
        }

        response = requests.post(url, headers=self.headers, data=payload)
        return response.json()

    # Modify Order
    def modify_order(
        self,
        order_id,
        order_type,
        quantity,
        price,
        trigger_price,
        disclosed_quantity,
        validity,
    ):
        url = f"{self.BASE_URL}/orders/regular/{order_id}"

        payload = {
            "order_type": order_type,
            "quantity": quantity,
            "price": price,
            "trigger_price": trigger_price,
            "disclosed_quantity": disclosed_quantity,
            "validity": validity,
        }

        response = requests.put(url, headers=self.headers, data=payload)
        return response.json()

    # Cancel Order
    def cancel_order(self, order_id):
        url = f"{self.BASE_URL}/orders/regular/{order_id}"

        response = requests.delete(url, headers=self.headers)
        return response.json()

    # Get All Orders
    def get_orders(self):
        url = f"{self.BASE_URL}/orders"

        response = requests.get(url, headers=self.headers)
        return response.json()

    # Get Order by ID
    def get_order_by_id(self, order_id):
        url = f"{self.BASE_URL}/orders/{order_id}"

        response = requests.get(url, headers=self.headers)
        return response.json()



"""
Zerodha Portfolio APIs
"""

import requests


class ZerodhaProtfolioAPI:

    BASE_URL = "https://api.kite.trade"

    def __init__(self, api_key: str, access_token: str) -> None:

        self.api_key = api_key
        self.access_token = access_token

        self.headers = {
            "X-Kite-ApiKey": self.api_key,
            "Authorization": f"token {self.api_key}:{self.access_token}",
        }



    # Holding contain the user's long term equity delivery stocks.
    def get_holdings(self):
        url = f"{self.BASE_URL}/portfolio/holdings"

        response = requests.get(url, headers=self.headers)

        return response.json()



    # Order book contain the user's pending orders.
    def get_order_book(self):
        url = f"{self.BASE_URL}/orders"

        response = requests.get(url, headers=self.headers)

        return response.json()

