# ==============================================================================

from fastapi import (
    APIRouter,
    status,
    Depends,
)
from fastapi.responses import JSONResponse, Response
import logging
import json
from typing import Optional

try:
    from . import auth, chart_helper as helper
    from db import get_pg_conn, get_redis_conn
except:
    from . import auth, chart_helper as helper
    from ..db import get_pg_conn, get_redis_conn

# ==============================================================================

router = APIRouter(prefix="/chart")
logger = logging.getLogger(__name__)

# ==============================================================================
# INSTRUMENTS


@router.get("/exchange/all")
async def exchange_all(
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
):
    payload = await helper.get_exchange_all(pg_conn=pg_conn)

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ------------------------------------------------------


@router.get("/segment/all")
async def segment_all(
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
    # redis_conn=Depends(get_redis_conn),
):
    payload = await helper.get_segment_all(pg_conn=pg_conn)

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ------------------------------------------------------


@router.get("/instrument/all")
async def instrument_all(
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
):

    payload = await helper.get_instrument_all(pg_conn=pg_conn)

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ------------------------------------------------------


@router.get("/instrument/filter")
async def instrument_filter(
    exchange: str,
    segment: str,
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
):

    payload = await helper.filter_instrument(
        pg_conn=pg_conn,
        exchange=exchange,
        segment=segment,
    )

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ------------------------------------------------------


@router.get("/instrument/search")
async def instrument_search(
    exchange: str,
    segment: str,
    trading_symbol: str,
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
):

    payload = await helper.search_instrument(
        pg_conn=pg_conn,
        exchange_text=exchange,
        segment_text=segment,
        trading_symbol_text=trading_symbol,
    )

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ------------------------------------------------------


@router.get("/instrument/detail")
async def instrument_detail(
    instrument_id: str,
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
):

    payload = await helper.get_instrument_detail(
        pg_conn=pg_conn,
        instrument_id=instrument_id,
    )

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ==============================================================================
# CHART DATA


@router.get("/data/bars_slots")
async def data_bars_slots(
    instrument_id: str,
    timeframe: str,
    timestamp_end: Optional[int] = None,
    email_id=Depends(auth.verify_access_token),
    pg_conn=Depends(get_pg_conn),
):
    payload = await helper.get_bars_slots(
        pg_conn=pg_conn,
        instrument_id=instrument_id,
        timeframe=timeframe,
        timestamp_end=timestamp_end,
    )

    return JSONResponse(
        content=json.dumps(payload, default=str),
        status_code=status.HTTP_200_OK,
        headers={},
    )


# ------------------------------------------------------
