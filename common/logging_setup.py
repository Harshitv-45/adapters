

import logging
import os
from datetime import datetime
from threading import Lock

# =============================================================================
# GLOBAL REGISTRY (one logger per broker+entity+component)
# =============================================================================

_LOGGER_REGISTRY = {}
_LOCK = Lock()


# =============================================================================
# FORMATTER
# =============================================================================

class TPOMSFormatter(logging.Formatter):
    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        level = record.levelname.ljust(9)
        entity = getattr(record, "entity", record.name)
        message = record.getMessage()
        return f"[{timestamp}] {level} [{entity}] {message}"


# =============================================================================
# LOGGER CREATION (ENTITY ONLY)
# =============================================================================

def _build_log_filename(broker, entity, component):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    return f"{broker.upper()}_{entity}_{component}_{ts}.log"


def get_or_create_logger(broker, entity, component="Adapter", log_dir="logs"):
    if not entity or entity.upper() == "SYSTEM":
        raise RuntimeError("SYSTEM logger is not allowed. SYSTEM logs must go to console only.")

    key = f"{broker}:{entity}"

    with _LOCK:
        if key in _LOGGER_REGISTRY:
            return _LOGGER_REGISTRY[key]

        os.makedirs(log_dir, exist_ok=True)

        logfile = os.path.join(log_dir, _build_log_filename(broker, entity, component))

        logger = logging.getLogger(key)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        formatter = TPOMSFormatter()

        # FILE HANDLER
        file_handler = logging.FileHandler(logfile, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # CONSOLE HANDLER
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        _LOGGER_REGISTRY[key] = logger
        return logger


# =============================================================================
# SIMPLE ENTITY LOGGER OBJECT
# =============================================================================

class EntityLogger:
    
    def __init__(self, broker, entity, component="Adapter"):
        self._logger = get_or_create_logger(broker, entity, component)
        self._entity = entity

    def info(self, msg, *args, **kwargs):
        self._logger.info(msg, *args, extra={"entity": self._entity}, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._logger.warning(msg, *args, extra={"entity": self._entity}, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._logger.error(msg, *args, extra={"entity": self._entity}, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._logger.exception(msg, *args, extra={"entity": self._entity}, **kwargs)
