import requests
import logging
import json

logger = logging.getLogger(__name__)

class MotilalPortfolioAPI:

    BASE_URL = "https://openapi.motilaloswal.com"
    
    def __init__(self, access_token, api_key, client_code):
        self.access_token = access_token
        self.api_key = api_key
        self.client_code = client_code

    def _headers(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "MOSL/V.1.1.0",
            "ApiKey": self.api_key,
            "vendorinfo": self.client_code,
            "Authorization": self.access_token,
            "accesstoken": self.access_token
        }
        return headers
    
    # Get Holdings
    def get_holdings(self):
        url = f"{self.BASE_URL}/rest/report/v1/getholdings"

        payload = {
            "clientcode": self.client_code
        }

        logger.info(f"[API REQUEST] {json.dumps(payload)}")

        response = requests.post(url, headers=self._headers(), json=payload)
        holdings_response = response.json()

        logger.info(f"[API RESPONSE] {json.dumps(holdings_response)}")

        # Check holdings retrieval status
        if holdings_response.get("status", "").upper() == "SUCCESS":
            logger.info("Holdings retrieved successfully")
        else:
            logger.warning(f"Holdings retrieval failed: {holdings_response}")
        
        return holdings_response
    
    # Get Positions
    def get_positions(self):
        url = f"{self.BASE_URL}/rest/report/v1/getpositions"

        payload = {
            "clientcode": self.client_code
        }

        logger.info(f"[API REQUEST] {json.dumps(payload)}")

        response = requests.post(url, headers=self._headers(), json=payload)
        positions_response = response.json()

        logger.info(f"[API RESPONSE] {json.dumps(positions_response)}")

        # Check positions retrieval status
        if positions_response.get("status", "").upper() == "SUCCESS":
            logger.info("Positions retrieved successfully")
        else:
            logger.warning(f"Positions retrieval failed: {positions_response}")
        
        return positions_response
