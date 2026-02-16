import time
import requests
import hashlib
import pyotp
from urllib.parse import urlparse, parse_qs

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


TOKEN_URL = "https://api.kite.trade"
BASE_URL = "https://kite.zerodha.com"


def get_request_token(api_key, user_id, password, totp_secret):
    login_url = f"{BASE_URL}/connect/login?v=3&api_key={api_key}"
    driver = None

    # Generate TOTP early (saves time later)
    #totp = pyotp.TOTP(totp_secret).now()

    try:
        chrome_options = Options()
        #chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.page_load_strategy = "eager"

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 10)

        driver.get(login_url)

        # API key validation
        try:
            userid = wait.until(
                EC.presence_of_element_located((By.ID, "userid"))
            )
        except TimeoutException:
            return None, "Invalid API Key"

        # Enter credentials
        userid.send_keys(user_id)
        driver.find_element(By.ID, "password").send_keys(password)

        # Submit password form
        driver.find_element(By.XPATH, "//form//button[@type='submit']").click()

        # Wait for TOTP input
        try:
            totp_input = wait.until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='number']"))
            )
        except TimeoutException:
            return None, "Invalid credentials"

        totp = pyotp.TOTP(totp_secret).now()
        # Enter TOTP
        totp_input.send_keys(totp)

        # Submit TOTP form
        driver.find_element(By.XPATH, "//form//button[@type='submit']").click()

        # Wait directly for redirect (no loop)
        wait.until(lambda d: "request_token" in d.current_url)

        parsed = urlparse(driver.current_url)
        params = parse_qs(parsed.query)
        request_token = params.get("request_token", [None])[0]
        return request_token, None

    except Exception:
        return None, "Invalid credentials"

    finally:
        if driver:
            driver.quit()


def exchange_token(api_key, api_secret, request_token):
    url = f"{TOKEN_URL}/session/token"

    checksum = hashlib.sha256(
        f"{api_key}{request_token}{api_secret}".encode()
    ).hexdigest()

    payload = {
        "api_key": api_key,
        "request_token": request_token,
        "checksum": checksum,
    }

    res = requests.post(url, data=payload)

    if res.status_code != 200:
        try:
            return None, res.json().get("message")
        except Exception:
            return None, res.text

    access_token = res.json()["data"]["access_token"]
    return access_token, None


def get_access_token(api_key, api_secret, user_id, password, totp_secret, logger=None):
    if not all([api_key, api_secret, user_id, password, totp_secret]):
        return None, "Missing required login credentials"

    request_token, error = get_request_token(
        api_key, user_id, password, totp_secret
    )

    if not request_token:
        return None, error

    access_token, error = exchange_token(
        api_key, api_secret, request_token
    )

    if not access_token:
        return None, error

    return access_token, None