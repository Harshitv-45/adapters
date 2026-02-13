import requests
import hashlib
import logging
import config

logger = logging.getLogger(__name__)


class MotilalAuthAPI:
    def __init__(self, api_key, client_code=None):
        self.api_key = api_key
        self.client_code = client_code
        self.access_token = None
        self.auth_token = None
        self.login_url = getattr(config, 'MOTILAL_LOGIN_URL', 'https://openapi.motilaloswal.com/rest/login/v3/authdirectapi')
        self.logout_url = 'https://openapi.motilaloswal.com/rest/login/v1/logout'

    def _password_hash(self, password, api_key):
        return hashlib.sha256((password + api_key).encode("utf-8")).hexdigest()

    def login(self, password, dob=None, client_code=None):
        try:
            client_id = client_code or self.client_code
            if not password or not self.api_key or not client_id:
                logger.warning("Missing credentials for login (PASSWORD, API_KEY, CLIENT_ID required)")
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
            
            logger.info(f"[API REQUEST] Performing login for client_id={client_id}")
            response = requests.post(self.login_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            login_response = response.json()
            logger.info(f"[API RESPONSE] {login_response}")
            
            if login_response.get("status", "").upper() == "SUCCESS":
                auth_token = (login_response.get("AuthToken") or 
                            login_response.get("authToken") or 
                            login_response.get("access_token"))
                if auth_token:
                    self.access_token = auth_token
                    self.auth_token = auth_token
                    logger.info("Login successful! AuthToken received.")
                    return {
                        "status": "SUCCESS",
                        "AuthToken": auth_token,
                        "access_token": auth_token,
                        "message": login_response.get("message", "Login successful")
                    }
                else:
                    logger.error("Login response missing AuthToken")
                    return {
                        "status": "ERROR",
                        "message": "Login response missing AuthToken"
                    }
            else:
                error_msg = login_response.get("message", "Unknown error")
                error_code = login_response.get("errorcode", "")
                logger.error(f"Login failed: {error_msg} (code: {error_code})")
                return {
                    "status": "ERROR",
                    "message": error_msg,
                    "errorcode": error_code
                }
                
        except Exception as e:
            logger.error(f"Login API call failed: {e}")
            return {
                "status": "ERROR",
                "message": str(e)
            }
    
    def logout(self, access_token=None, client_code=None):
        try:
            client_id = client_code or self.client_code
            token = access_token or self.access_token or self.auth_token
            
            if not token or not self.api_key or not client_id:
                logger.warning("Missing credentials for logout (access_token, API_KEY, CLIENT_ID required)")
                return {
                    "status": "ERROR",
                    "message": "Missing credentials for logout"
                }
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "ApiKey": self.api_key,
                "vendorinfo": client_id,
                "Authorization": token,
                "accesstoken": token,
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
            
            payload = {
                "userid": client_id
            }
            
            logger.info(f"[API REQUEST] Performing logout for client_id={client_id}")
            response = requests.post(self.logout_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            logout_response = response.json()
            logger.info(f"[API RESPONSE] {logout_response}")
            
            if logout_response.get("status", "").upper() == "SUCCESS":
                self.access_token = None
                self.auth_token = None
                logger.info("Logout successful!")
                return {
                    "status": "SUCCESS",
                    "message": logout_response.get("message", "Logout successful")
                }
            else:
                error_msg = logout_response.get("message", "Unknown error")
                error_code = logout_response.get("errorcode", "")
                logger.error(f"Logout failed: {error_msg} (code: {error_code})")
                return {
                    "status": "ERROR",
                    "message": error_msg,
                    "errorcode": error_code
                }
                
        except Exception as e:
            logger.error(f"Logout API call failed: {e}")
            return {
                "status": "ERROR",
                "message": str(e)
            }