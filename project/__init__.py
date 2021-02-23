import logging
import redis
import pytz
import os
from datetime import datetime
from urllib.parse import urlparse

logging.basicConfig(format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

r = redis.from_url(os.getenv("REDIS_URL"), charset="utf-8", decode_responses=True)

berlin_tz = pytz.timezone("Europe/Berlin")
now = datetime.now(tz=berlin_tz)
