"""
Zerodha Instruments Module (PUBLIC)

- Downloads Zerodha instruments CSV (no auth required)
- Caches on disk + memory
- Safe for all users
"""

import os
import requests
import pandas as pd
from typing import Optional


class ZerodhaInstruments:
    # Public endpoint (NO AUTH REQUIRED)
    INSTRUMENTS_URL = "https://api.kite.trade/instruments"
    DATA_DIR = "data"
    FILE_PATH = os.path.join(DATA_DIR, "zerodha_instruments.csv")

    _df: Optional[pd.DataFrame] = None
    _loaded = False

    def __init__(self, logger=None):
        self.logger = logger
        os.makedirs(self.DATA_DIR, exist_ok=True)

    # ---------------------------------------------------------
    # DOWNLOAD (PUBLIC)
    # ---------------------------------------------------------
    def download_instruments(self, force: bool = False) -> bool:
        if os.path.exists(self.FILE_PATH) and not force:
            if self.logger:
                self.logger.info("[INSTRUMENTS] CSV already exists")
            return True

        try:
            if self.logger:
                self.logger.info("[INSTRUMENTS] Downloading instruments CSV (public endpoint)")

            resp = requests.get(self.INSTRUMENTS_URL, timeout=30)
            resp.raise_for_status()

            with open(self.FILE_PATH, "wb") as f:
                f.write(resp.content)

            ZerodhaInstruments._df = None
            ZerodhaInstruments._loaded = False

            if self.logger:
                self.logger.info("[INSTRUMENTS] Download successful")

            return True

        except Exception as e:
            if self.logger:
                self.logger.warning(f"[INSTRUMENTS] Download failed: {e}")
            return False

    # ---------------------------------------------------------
    # LOAD INTO MEMORY
    # ---------------------------------------------------------
    def load_instruments(self) -> bool:
        if ZerodhaInstruments._loaded:
            return True

        if not os.path.exists(self.FILE_PATH):
            if self.logger:
                self.logger.warning("[INSTRUMENTS] CSV not found")
            return False

        try:
            df = pd.read_csv(self.FILE_PATH)

            df["exchange_token"] = df["exchange_token"].astype(int)
            df.set_index("exchange_token", inplace=True)

            ZerodhaInstruments._df = df
            ZerodhaInstruments._loaded = True

            if self.logger:
                self.logger.info(f"[INSTRUMENTS] Loaded {len(df)} rows")

            return True

        except Exception as e:
            if self.logger:
                self.logger.warning(f"[INSTRUMENTS] Load failed: {e}")
            return False

    # ---------------------------------------------------------
    # LOOKUPS
    # ---------------------------------------------------------
    def get_tradingsymbol(self, exchange_token: int) -> Optional[str]:
        if not ZerodhaInstruments._loaded:
            self.load()

        df = ZerodhaInstruments._df
        if df is None:
            return None

        try:
            value = df.loc[exchange_token, "tradingsymbol"]

            # Handle duplicate tokens (Series)
            if isinstance(value, pd.Series):
                return value.iloc[0]

            return value

        except Exception:
            return None

    def get_instrument(self, exchange_token: int) -> Optional[dict]:
        if not ZerodhaInstruments._loaded:
            self.load()

        df = ZerodhaInstruments._df
        if df is None:
            return None

        try:
            row = df.loc[exchange_token]

            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]

            return row.to_dict()

        except Exception:
            return None
