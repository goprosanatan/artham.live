# ===================================================================================================
# CLI - BETA
# ===================================================================================================

import os
import logging
from zoneinfo import ZoneInfo
from decouple import config
import psycopg
import psycopg_pool
from psycopg.rows import class_row, dict_row
from psycopg.sql import SQL, Identifier, Composed, Placeholder
import numpy as np
import pandas as pd
import redis
from contextlib import contextmanager
import time
import threading
import math
from math import sqrt, exp, log, pi
from scipy.stats import norm
from dateutil.relativedelta import relativedelta
import pathlib
import sys
from collections import deque
import requests
from dateutil.tz import tzoffset
import shutil
import random
import jwt
import pytz
from flatten_json import flatten
import json
import multiprocessing
import re
import psutil
from pydantic import BaseModel, Field, field_validator
from decimal import Decimal
from typing import Optional, Any, List, Dict, Literal, Type, Sequence, Literal
import math
from datetime import datetime, timedelta, timezone, date
from dataclasses import dataclass
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import functools
import os
import csv


# # Multiprocessing can only use physical cores. so maximum allowed is given as following
# NUM_MULTIPROCESS = psutil.cpu_count(logical=False)

os.chdir(config("DIR_PROJECT", cast=str))

logging.basicConfig(
    filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_00_cli.log")),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

from library import models
from library.adapters.groww_api import GROWW_API
from library.adapters.groww_websocket import GROWW_WEBSOCKET
from library.adapters.kite_api import KITE_API
from library.modules import misc, pg_crud, redis_crud
from library.core.bar import BAR_RESAMPLER, BAR_LOADER, BAR_DOWNLOADER, BAR_DELETER
from library.core.instrument import INSTRUMENT_SEARCH
from library.core.calendar import CALENDAR_LOADER
from library.core.option import EXPIRY

# ===================================================================================================
# CONNECTIONS

pg_conn = psycopg.connect(
    dbname="artham",
    user=config("POSTGRES_USER", cast=str), 
    password=config("POSTGRES_PASSWORD", cast=str), 
    host=config("POSTGRES_HOST", cast=str), 
    port=config("POSTGRES_PORT", cast=int), 
    options=f"-c timezone=Asia/Kolkata",
)

# postgres ---- test
pg_conn.commit()

redis_conn = redis.Redis(
    host=config("REDIS_HOST", cast=str),
    port=config("REDIS_PORT", cast=int),
    decode_responses=True,
)

# redis_conn.flushall()

# ===================================================================
# KITE INSTRUMENTS
# ===================================================================

finder = INSTRUMENT_SEARCH(pg_conn=pg_conn)

list_eq = finder.filter(trading_symbol="RELIANCE")
list_eq = [i.instrument_id for i in list_eq]

list_opt = finder.filter(
    exchange="NFO",
    segment="NFO-OPT",
    underlying_trading_symbol="RELIANCE",
)
list_opt = [i.instrument_id for i in list_opt]

list_fut = finder.filter(
    exchange="NFO",
    segment="NFO-FUT",
    underlying_trading_symbol="RELIANCE",
)
list_fut = [i.instrument_id for i in list_fut]

list_all = list_eq + list_opt + list_fut


downloader = BAR_DOWNLOADER(
    access_token=redis_conn.get("kite:access_token"),
    pg_conn=pg_conn,
    schema_name="public",
    table_name="bars",
)

timeframes = ["1m"]

for instrument_id in list_eq:

    # instrument_id =128083204
    for timeframe in timeframes:
        print(
            f"Downloading external bars for instrument_id: {instrument_id}, timeframe: {timeframe}"
        )

        try:
            downloader.download_bars(
                instrument_id=instrument_id,
                timeframe=timeframe,
            )
        except Exception as e:
            print(f"Error downloading bars for instrument_id {instrument_id}: {e}")


deleter = BAR_DELETER(
    pg_conn=pg_conn,
    schema_name="public",
    table_name="bars",
)

deleter.delete_by_instrument_timeframe(
    instrument_id=128083204,
    timeframe="1m",
)

deleter.delete_by_instrument_timeframe(
    instrument_id=738561,
    timeframe="1m",
)

deleter.delete_after_timestamp(
    dt=datetime(2026, 2, 19, 00, 00, tzinfo=ZoneInfo("Asia/Kolkata")),
)



# ===================================================================
# KITE CONNECT
# ===================================================================

kite = KiteConnect(api_key=config("KITE_API_KEY", cast=str))

kite.login_url()

data = kite.generate_session(
    "QWFluHAjYUEBaYQ2szsZKC77N3ngutIK",
    api_secret=config("KITE_API_SECRET", cast=str),
)

data["access_token"]

access_token = "nHHBzflcvbwwNCjBQYDv67yA0GKIgRMU"
kite_api = KITE_API(access_token=access_token)

redis_conn.set("kite:access_token", access_token)




# ===================================================================




#  Create decay factor array wrt time in seconds




# read all instruments from set as int
instruments_all = redis_conn.smembers("instruments:reliance:all")





crud_pydantic = pg_crud.GENERAL(timezone="Asia/Kolkata", pg_conn=pg_conn)


crud_pydantic.table_get(
    db_name="artham",
    schema_name="public",
    table_name="ticks",
)


crud_pydantic.table_get_row_count(
    db_name = "postgres",
    schema_name = "public",
    table_name = "ticks",
)
