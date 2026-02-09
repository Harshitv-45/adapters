import redis
import json
import logging
import config

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self._connection = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
        )
        self._channel = config.CH_BLITZ_RESPONSES 

    @property
    def connection(self):
        return self._connection

    @property
    def channel(self):
        return self._channel

    def publish(self, message, default=str):
        """Publish message to the default Redis channel only."""
        try:
            payload = json.dumps(message, default=default) if not isinstance(message, str) else message
            self._connection.publish(self._channel, payload)
            logger.debug(f"Published to {self._channel}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish to {self._channel}: {e}")
            return False
