import json
import requests
import hashlib
import config



class MotilalAuthAPI:
    def __init__(self, api_key, client_code, password=None, dob=None, logger = None):
        """
        Initialize and optionally login instantly.
        :param api_key: API key from Motilal
        :param client_code: Client code
        :param password: Login password
        :param dob: DOB / 2FA if required
        """
        self.api_key = api_key
        self.client_code = client_code
        self.password = password
        self.dob = dob
        self.access_token = None
        self.auth_token = None
        self.logger = logger


        self.login_url = getattr(
            config, 
            'MOTILAL_LOGIN_URL', 
            'https://openapi.motilaloswal.com/rest/login/v3/authdirectapi'
        )
        self.logout_url = 'https://openapi.motilaloswal.com/rest/login/v1/logout'

        # Attempt login immediately if password is provided
        if self.password:
            self.login(password=self.password, dob=self.dob)

    def _password_hash(self, password, api_key):
        return hashlib.sha256((password + api_key).encode("utf-8")).hexdigest()

    def login(self, password=None, dob=None, client_code=None):
        """Perform login and store access_token/auth_token internally"""
        client_id = client_code or self.client_code
        password = password or self.password
        dob = dob or self.dob

        if not client_id or not password or not self.api_key:
            self.logger.warning("Missing credentials for login (PASSWORD, API_KEY, CLIENT_ID required)")
            return None

        payload = {
            "userid": client_id,
            "password": self._password_hash(password, self.api_key),
            "2FA": dob or ""
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ApiKey": self.api_key,
            "vendorinfo": client_id,
            "User-Agent": "MOSL/V.1.1.0",
            "SourceId": "WEB",
            "ClientLocalIp": "127.0.0.1",
            "ClientPublicIp": "127.0.0.1",
            "MacAddress": "00:11:22:33:44:55",
            "osname": "Windows",
            "osversion": "10",
            "devicemodel": "PC",
            "manufacturer": "Generic",
            "productname": "Algo",
            "productversion": "1.0",
            "browsername": "Chrome",
            "browserversion": "120"
        }

        try:
            self.logger.info(f"[API REQUEST] Performing login for client_id={client_id}")
            response = requests.post(self.login_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            login_response = response.json()
            self.logger.info("[API RESPONSE]~%s",json.dumps(login_response))

            if login_response.get("status", "").upper() == "SUCCESS":
                auth_token = login_response.get("AuthToken") or login_response.get("access_token")
                if auth_token:
                    self.access_token = auth_token
                    self.auth_token = auth_token
                    self.logger.info("Login successful! AuthToken received.")
                    return auth_token
                else:
                    self.logger.error("Login response missing AuthToken")
            else:
                error_msg = login_response.get("message", "Unknown error")
                error_code = login_response.get("errorcode", "")
                self.logger.error(f"Login failed: {error_msg} (code: {error_code})")
        except Exception as e:
            self.logger.error(f"Login API call failed: {e}", exc_info=True)
        return None
    
    def logout(self):
        """Logout from Motilal production API using client_code as userid"""
        if not self.access_token:
            if self.logger:
                self.logger.warning("No active session to logout")
            return None

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ApiKey": self.api_key,
            "AuthToken": self.access_token,
        }

        # Always send the client_code as userid
        payload = {"userid": self.client_code}

        try:
            if self.logger:
                self.logger.info(f"Logging out client_id={self.client_code} from Motilal production API")
            response = requests.post(self.logout_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            logout_response = response.json()
            if self.logger:
                self.logger.info(f"[LOGOUT RESPONSE] {logout_response}")

            # Clear tokens
            self.access_token = None
            self.auth_token = None

            return logout_response
        except Exception as e:
            if self.logger:
                self.logger.error(f"Logout API call failed: {e}", exc_info=True)
            return None
