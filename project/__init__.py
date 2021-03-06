import logging
import os
from datetime import datetime
from urllib.parse import urlparse

import pytz
import redis

log_level = os.getenv("LOG_LEVEL", "ERROR").upper()
logging.basicConfig(
    level=log_level, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

r = redis.from_url(os.getenv("REDIS_URL"), charset="utf-8", decode_responses=True)

berlin_tz = pytz.timezone("Europe/Berlin")
now = datetime.now(tz=berlin_tz)
