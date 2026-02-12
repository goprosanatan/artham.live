# ===================================================================================================
# CHART - HELPER
# ===================================================================================================

import pandas as pd
import numpy as np
import json
import logging
import datetime
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from typing import Optional, Sequence, Literal, List

from library.core.instrument import INSTRUMENT_SEARCH_ASYNC
from library.modules.pg_crud import WITH_PYDANTIC_ASYNC
from library.core.bar import BAR_LOADER_ASYNC
from library import models
from library.core.calendar import CALENDAR_LOADER

# ===================================================================================================
# INSTRUMENTS


async def get_exchange_all(pg_conn: psycopg.AsyncConnection):

    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)

    return await instrument_search.unique(column_name="exchange")


# ------------------------------------------------------


async def get_segment_all(pg_conn: psycopg.AsyncConnection):

    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)

    return await instrument_search.unique(column_name="segment")


# ------------------------------------------------------


async def get_instrument_all(pg_conn: psycopg.AsyncConnection):

    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)

    result = await instrument_search.get_all()
    json_response = [item.model_dump() for item in result]
    return json_response


# ------------------------------------------------------


async def get_instrument_detail(pg_conn: psycopg.AsyncConnection, instrument_id: str):

    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)

    result = await instrument_search.filter(instrument_id=instrument_id)

    if not result:
        return {}

    data = result[0].model_dump()

    return data


# ------------------------------------------------------


async def search_instrument(
    pg_conn: psycopg.AsyncConnection,
    exchange_text: str,
    segment_text: str,
    trading_symbol_text: str,
):
    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)

    result = await instrument_search.search(
        exchange=exchange_text,
        segment=segment_text,
        trading_symbol=trading_symbol_text,
    )

    response = [item.model_dump() for item in result]

    return response


# ------------------------------------------------------


async def filter_instrument(
    pg_conn: psycopg.AsyncConnection,
    exchange: str,
    segment: str,
):

    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)

    result = await instrument_search.filter(
        exchange=exchange,
        segment=segment,
    )
    response = [item.model_dump() for item in result]

    return response


# ===================================================================================================
# BARS and SLOTS


async def get_bars_slots(
    pg_conn: psycopg.AsyncConnection,
    instrument_id: str,
    timeframe: str,
    timestamp_end: Optional[int] = None,
):
    instrument_search = INSTRUMENT_SEARCH_ASYNC(pg_conn=pg_conn)
    instrument_info = await instrument_search.filter(instrument_id=instrument_id)

    if not instrument_info:
        raise ValueError(f"Instrument not found for id : {instrument_id}")

    exchange = instrument_info[0].exchange

    loader = BAR_LOADER_ASYNC(
        pg_conn=pg_conn,
        schema_name="public",
        table_name="bars",
    )

    bars = await loader.load_bars(
        instrument_id=instrument_id,
        timeframe=timeframe,
        timestamp_end=timestamp_end,  # Unix timestamp in milliseconds, or None for latest
    )

    slots: List[int] = []
    all_slots: List[datetime.datetime] = []

    if bars:
        first_bar_date = bars[0]["bar_ts"]
        last_bar_date = bars[-1]["bar_ts"]

        # build past slots between first and last returned bar using calendar helper
        all_slots = CALENDAR_LOADER.session_slots_between(
            exchange=exchange,
            timeframe=timeframe,
            start_dt=first_bar_date,
            end_dt=last_bar_date,
        )

        if timestamp_end is None:
            all_slots += CALENDAR_LOADER.session_slots_after(
                exchange=exchange,
                timeframe=timeframe,
                now=last_bar_date,
            )

    # prepare bars and slots response
    slots = [int(dt.timestamp() * 1000) for dt in all_slots]

    for row in bars:
        row["bar_ts"] = int(
            row["bar_ts"].timestamp() * 1000
        )  # Convert to timestamp milliseconds

    return {
        "bars": bars,
        "slots": slots,
    }


# ------------------------------------------------------
