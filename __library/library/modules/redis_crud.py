# ===================================================================================================
# CRUD - REDIS
# ===================================================================================================

import os
import redis
import numpy as np
import pandas as pd
import logging
from decouple import config

logger = logging.getLogger(__name__)

# ===================================================================================================


class GENERAL:

    def __init__(self):
        pass

    def connect(
        self,
        db_number: int = 0,
    ):

        redis_conn = redis.Redis(
            host=config("REDIS_HOST", cast=str),
            port=config("REDIS_PORT", cast=str),
            # password=config("REDIS_PASSWORD", cast=str),
            db=db_number,
            decode_responses=True,
        )

        # test connection
        redis_conn.ping()

        return redis_conn

    def decode_keyspace_notifications(self, message):

        channel = message["channel"]
        data = message["data"]

        info = {"key_name": None, "db_number": None, "operation": None}

        if "__keyspace@" in channel:

            channel = channel.replace("__keyspace@", "")
            db_number__, key_name = channel.split(":", 1)
            db_number = db_number__.replace("__", "")

            info = {"key_name": key_name, "db_number": db_number, "operation": data}

        elif "__keyevent@" in channel:

            channel = channel.replace("__keyevent@", "")
            db_number__, operation = channel.split(":", 1)
            db_number = db_number__.replace("__", "")

            info = {"key_name": data, "db_number": db_number, "operation": operation}

        return info

# --------------------------------------------------------------------------------------------------

