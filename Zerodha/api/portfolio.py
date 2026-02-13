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

