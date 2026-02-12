# ===================================================================================================
# CLI - BETA
# ===================================================================================================

import os
import logging
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
import asyncio
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
from typing import Optional, Any, List, Dict, Literal, Type, Union
import math
from datetime import datetime, timedelta, timezone, date
from dataclasses import dataclass
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker

from user_api import db


# # Multiprocessing can only use physical cores. so maximum allowed is given as following
# NUM_MULTIPROCESS = psutil.cpu_count(logical=False)

os.chdir("/Users/node/__Code/artham.live")

logging.basicConfig(
    filename="/Users/node/__Server/volumes/logs/artham_cli.log",
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
from library.core.bar import BAR_RESAMPLER, BAR_DOWNLOADER
from library.core.instrument import INSTRUMENT_SEARCH, INSTRUMENT_DOWNLOADER

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


crud_pydantic = pg_crud.WITH_PYDANTIC(timezone="Asia/Kolkata", pg_conn=pg_conn)


crud_pydantic.table_delete(
    db_name="artham",
    schema_name="public",
    table_name="ticks",
)



pg_conn.rollback()


crud_pydantic.table_create(
    db_name="artham",
    schema_name="public",
    table_name="ticks",
    model=models.STANDARD_TICK_MD5,
)




crud_pydantic.table_get(
    db_name="artham",
    schema_name="public",
    table_name="ticks",
)



crud_pydantic.table_create(
    db_name="artham",
    schema_name="public",
    table_name="ticks",
    model=models.STANDARD_TICK_MD5,
)

crud_pydantic.table_to_timescaledb_hypertable(
    db_name="artham",
    schema_name="public",
    table_name="ticks",
    time_column="exchange_ts",
    chunk_time_interval="1 hour",
)


# crud_pydantic.table_set_column_value(
#     db_name="artham",
#     schema_name="public",
#     table_name="instruments",
#     column_name="expired",
#     value=True,
# )


# crud_pydantic.table_upsert(
#     db_name="artham",
#     schema_name="public",
#     table_name="instruments",
#     list_models=df_master,
#     model=models.STANDARD_INSTRUMENT,
# )


# pg_one = pg_crud.GENERAL(timezone="Asia/Kolkata")

# pg_one.table_delete(

#     db_name="artham",
#     schema_name="public",
#     table_name="instruments",
# )

# crud_pydantic = pg_crud.WITH_PYDANTIC(timezone="Asia/Kolkata")

# crud_pydantic.table_create(
#     db_name="artham",
#     schema_name="public",
#     table_name="ticks",
#     model=models.STANDARD_TICK_MD5,
# )

# crud_pydantic.table_delete(
#     db_name="artham",
#     schema_name="public",
#     table_name="ticks",
# )


# pg_one = pg_crud.GENERAL(timezone="Asia/Kolkata")


# pg_one.table_delete(
#     db_name="artham",
#     schema_name="public",
#     table_name="ticks",
# )

# pg_one = pg_crud.GENERAL(timezone="Asia/Kolkata")

# pg_one.database_create(
#     db_name="artham",
#     timescaledb_extension=True,
# )


# crud_pydantic.table_insert(
#     db_name="artham",
#     schema_name="public",
#     table_name="ticks",
#     model=models.STANDARD_TICK_MD5,
#     list_models=ticks_ingestable,
# )


# def load_reliance_instruments(pg_conn):
#     """Resolve instrument tokens for Reliance and related derivatives.

#     Uses the project's INSTRUMENT_SEARCH helper to pull symbols from Postgres and
#     then converts them into "kite scrips" with instrument_token values. Returns
#     a Python list[int] of tokens to subscribe to.
#     """

#     finder = INSTRUMENT_SEARCH(pg_conn)

#     list_all = []

#     list_all += finder.filter(
#         trading_symbol="RELIANCE"
#     )

#     list_all += finder.search(
#         exchange="NFO",
#         underlying_trading_symbol="RELIANCE",
#     )

#     list_all += finder.search(
#         exchange="BFO",
#         underlying_trading_symbol="RELIANCE",
#     )

#     list_all = pd.DataFrame([s.model_dump() for s in list_all])

#     list_all = list_all.instrument_id.tolist()

#     return list_all


# # ===================================================================
# # KITE CONNECT
# # ===================================================================


# from kite_transform import transform_kite_instruments


# kite_api = KiteConnect(api_key=config("KITE_API_KEY", cast=str))
# kite_api.set_access_token("C5GSejDpemZpdwncoV194sdffwtJL38W")


# all_instruments = kite_api.instruments()

# df_original = pd.DataFrame(all_instruments)


# df_altered = df_master


# df_altered = transform_kite_instruments(all_instruments)
# # You


# # replace df_original Empty strings or <NA> with None
# df_original = df_original.replace(["", pd.NA], None)

# i = 0

# # iterate through df_original and compare with df_altered -- columns to compare tradingsymbol-trading_symbol, expiry-expiry, instrument_type-option_type, name-description
# for idx, row in df_original.iterrows():
#     altered_row = df_altered.iloc[idx]


#     # if altered_row['option_type'] is None:
#     #     continue

#     # if altered_row['underlying_instrument_id'] is None:
#     #     continue

#     if altered_row["trading_symbol"] == "RELIANCE" or "RELIANCE" in altered_row["trading_symbol"]:


#         # if row['instrument_type'] == 'EQ':
#         #     continue

#         print(f"{altered_row['trading_symbol']} {altered_row['underlying_instrument_id']}")
#         print(f"{row['expiry']} vs {altered_row['expiry']}")
#         print(f"{row['instrument_type']}")
#         print(f"{row['name']} vs {altered_row['description']}")

#         print("----\n\n")

#         i+=1

#     else:
#         continue

#     print(i)


kite = KiteConnect(api_key=config("KITE_API_KEY", cast=str))

kite.login_url()

data = kite.generate_session(
    "OyMKCz4z5G3re6f2uH8UhGCheIZ7MLU4",
    api_secret=config("KITE_API_SECRET", cast=str),
)

data["access_token"]

access_token = "qZi06BmekFAIMEJn7Nrp3V7sjBvwhaVp"
kite_api = KITE_API(access_token=access_token)


access_token = "nFBx5vWmGEOU89MaKhXrqbOG28lfvRtm"
redis_conn.set("kite:access_token", access_token)

redis_conn.flushall()


def load_reliance_instruments(pg_conn):

    finder = INSTRUMENT_SEARCH(pg_conn)

    list_all = []

    list_all += finder.filter(trading_symbol="RELIANCE")

    list_all += finder.search(
        exchange="NFO",
        underlying_trading_symbol="RELIANCE",
    )

    list_all = pd.DataFrame([s.model_dump() for s in list_all])

    list_all = list_all.instrument_id.tolist()

    return list_all


list_all = load_reliance_instruments(pg_conn)


def ingest_ticks(ticks):

    timestamp_now = datetime.now()
    ticks_ingestable = []

    for tick in ticks:
        tick_flat = flatten(tick)
        tick_flat["receive_ts"] = timestamp_now

        ticks_ingestable.append(models.KITE_TICK_FULL(**tick_flat))

    with_pydantic = pg_crud.WITH_PYDANTIC(timezone="Asia/Kolkata")

    with_pydantic.table_insert(
        db_name="artham",
        schema_name="public",
        table_name="ticks",
        model=models.KITE_TICK_FULL,
        list_models=ticks_ingestable,
    )


access_token = "g6JsFOdcFNVrGGN6wM5WnXkAKGVD1O4E"

# Initialise
kws = KiteTicker(
    config("KITE_API_KEY", cast=str),
    access_token,
)


def on_ticks(ws, ticks):
    # Callback to receive ticks.
    # print("Ticks: {}".format(ticks))
    # print(len(ticks))

    ingest_ticks(ticks)


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    list_all = load_reliance_instruments(pg_conn)
    ws.subscribe(list_all)
    # ws.subscribe([128083204, 738561])  # RELIANCE

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, list_all)

    # # Set RELIANCE to tick in `full` mode.
    # ws.set_mode(ws.MODE_LTP, [738561])

    pass


def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    # ws.stop()
    pass


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=True)
kws.resubscribe()

kws.close()


kite_api = KITE_API(access_token="qZi06BmekFAIMEJn7Nrp3V7sjBvwhaVp")

kite_api.get_instruments()


kite_api.get_historical(
    instrument_id=12803586,
    from_date=datetime(2025, 11, 11),
    to_date=datetime.now(),
    interval="1D",
)

bars = kite_api.get_historical_max(
    instrument_id=738561,
    interval="1m",
)

bars2 = kite_api.get_historical_max(
    instrument_id=738561,
    interval="1D",
)

# convert bars to dataframe
df_bars = pd.DataFrame([b.model_dump() for b in bars])

# convert bars to dataframe
df_bars2 = pd.DataFrame([b.model_dump() for b in bars2])

# ensure datetime dtype
df_bars["bar_ts"] = pd.to_datetime(df_bars["bar_ts"])

is_chrono = df_bars["bar_ts"].is_monotonic_increasing
print("Chronological (oldest→newest)?", is_chrono)


#  confirm if bar_ts are in chronological order


is_strict = df_bars["bar_ts"].is_monotonic_increasing and df_bars["bar_ts"].is_unique
print("Strict chronological with no duplicates?", is_strict)


# find out duplicates if any
duplicates = df_bars[df_bars.duplicated(subset=["bar_ts"], keep=False)]
print("Duplicates:\n", duplicates)


resampler = BAR_RESAMPLER(
    bars=bars,
)
# resampler.resample_day(timeframe="6M")

resampler.resample_minute(minutes=3)


resampler = BAR_RESAMPLER(bars=bars2)
resampler.resample_day(timeframe="1W")

resampler.resample_minute(minutes=3)


access_token = redis_conn.get("kite:access_token")


downloader = INSTRUMENT_DOWNLOADER(
    access_token=access_token,
    pg_conn=pg_conn,
)

downloader.download_instruments()


downloader = BAR_DOWNLOADER(
    access_token=access_token,
    pg_conn=pg_conn,
    table_name="bars_external",
)

list_all = load_reliance_instruments(pg_conn)

list_all = [128083204, 738561]

for instrument_id in list_all:
    num_bars = downloader.download_bars(
        instrument_id=instrument_id,
        timeframe="1D",
    )


num_bars = downloader.download_bars(
    instrument_id=738561,
    timeframe="1D",
)


crud_pydantic = pg_crud.WITH_PYDANTIC(timezone="Asia/Kolkata", pg_conn=pg_conn)


crud_pydantic.table_delete(
    db_name="artham",
    schema_name="public",
    table_name="instruments",
)

pg_conn.rollback()


bar_3m = crud_pydantic.mview_get(
    db_name="artham",
    schema_name="public",
    materialized_view_name="bars_external_3m",
)

bar_3m

bar_3m = crud_pydantic.mview_get(
    db_name="artham",
    schema_name="public",
    materialized_view_name="bars_external_1w",
)

bar_3m


bar_3m = crud_pydantic.mview_get(
    db_name="artham",
    schema_name="public",
    materialized_view_name="bars_external_1y",
)

bar_3m


# -- Refresh all historical data for each view
# CALL refresh_continuous_aggregate('bars_external_3m', NULL, NULL);
# CALL refresh_continuous_aggregate('bars_external_1y', NULL, NULL);
# -- ... repeat for all views

# SELECT DISTINCT bar_ts
# FROM bars_external_1y
# ORDER BY bar_ts DESC
# LIMIT 3;


# CALL refresh_continuous_aggregate('bars_external_1w', '2023-01-01', '2026-01-01');
