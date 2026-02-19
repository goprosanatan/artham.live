# ===================================================================
# Day Start Script
# ===================================================================

import os
import logging
from decouple import config
import psycopg
import redis
import sys
from dataclasses import dataclass
from kiteconnect import KiteConnect, exceptions as kiteconnect_exceptions

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_00_scripts.log")),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s DAY_START %(levelname)-8s %(message)s",
)

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

# Test Postgres connection
try:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        print("Postgres connection test result:", result)
        logger.info(f"Postgres connection test result: {result}")
except Exception as e:
    print(f"Postgres connection failed: {e}")
    logger.error(f"Postgres connection failed: {e}")


redis_conn = redis.Redis(
    host=config("REDIS_HOST", cast=str),
    port=config("REDIS_PORT", cast=int),
    decode_responses=True,
)

# Test Redis connection
try:
    pong = redis_conn.ping()
    print(
        "Redis connection test result: Pong" if pong else "Redis connection test failed"
    )
    logger.info(
        "Redis connection test result: Pong" if pong else "Redis connection test failed"
    )
except Exception as e:
    print(f"Redis connection failed: {e}")
    logger.error(f"Redis connection failed: {e}")

# ===================================================================================================
# Kite Connect session

kite = KiteConnect(api_key=config("KITE_API_KEY", cast=str))
request_token = redis_conn.get("kite:request_token")

try:
    data = kite.generate_session(
        request_token=request_token,
        api_secret=config("KITE_API_SECRET", cast=str),
    )
    kite.set_access_token(data["access_token"])
    redis_conn.set("kite:access_token", data["access_token"])
    print("Access token set successfully.")
    logger.info("Access token set successfully.")
except kiteconnect_exceptions.InputException:
    print(f"Invalid request token. Please generate a new one. '{request_token}'")
    logger.error(f"Invalid request token. Please generate a new one. '{request_token}'")
    sys.exit(1)
except Exception as e:
    print(f"Error setting access token: {e}")
    logger.error(f"Error setting access token: {e}")
    sys.exit(1)


# ===================================================================================================
# Instrument Download

downloader_instrument = INSTRUMENT_DOWNLOADER(
    access_token=redis_conn.get("kite:access_token"),
    pg_conn=pg_conn,
)
downloader_instrument.download_instruments()

# ===================================================================================================
# Instrument Loading in Redis (Plan 1: hashes + index sets)

finder = INSTRUMENT_SEARCH(pg_conn=pg_conn)

list_all_models = finder.get_all()

def _clear_instrument_namespace(conn):
    cursor = 0
    keys = []
    for key in conn.scan_iter(match="instruments:*", count=500):
        keys.append(key)
        if len(keys) >= 500:
            conn.delete(*keys)
            keys.clear()
    if keys:
        conn.delete(*keys)

def _inst_hash(model):
    # Normalize to strings for Redis hashes
    def _s(val):
        if val is None:
            return ""
        if hasattr(val, "isoformat"):
            return val.isoformat()
        return str(val)

    return {
        "instrument_id": _s(model.instrument_id),
        "exchange": _s(getattr(model, "exchange", "")),
        "segment": _s(getattr(model, "segment", "")),
        "trading_symbol": _s(getattr(model, "trading_symbol", "")),
        "underlying_instrument_id": _s(getattr(model, "underlying_instrument_id", "")),
        "underlying_trading_symbol": _s(getattr(model, "underlying_trading_symbol", "")),
        "timezone": _s(getattr(model, "timezone", "")),
        "instrument_type": _s(getattr(model, "instrument_type", "")),
        "description": _s(getattr(model, "description", "")),
        "isin": _s(getattr(model, "isin", "")),
        "strike": _s(getattr(model, "strike", "")),
        "expiry": _s(getattr(model, "expiry", "")),
        "lot_size": _s(getattr(model, "lot_size", "")),
        "tick_size": _s(getattr(model, "tick_size", "")),
        "display_order": _s(getattr(model, "display_order", "")),
        "expired": _s(getattr(model, "expired", "")),
    }

def _add_indexes(conn, model):
    iid = str(model.instrument_id)
    exchange = (getattr(model, "exchange", "") or "").lower()
    segment = (getattr(model, "segment", "") or "").lower()
    instrument_type = (getattr(model, "instrument_type", "") or "").lower()
    trading_symbol = (getattr(model, "trading_symbol", "") or "").lower()
    underlying_id = getattr(model, "underlying_instrument_id", None)
    underlying_symbol = (getattr(model, "underlying_trading_symbol", "") or "").lower()
    expiry_val = getattr(model, "expiry", None)

    conn.sadd("instruments:all", iid)

    if exchange:
        conn.sadd(f"instruments:exchange:{exchange}", iid)
    if segment:
        conn.sadd(f"instruments:segment:{segment}", iid)
    if instrument_type:
        conn.sadd(f"instruments:type:{instrument_type}", iid)
    if trading_symbol:
        conn.sadd(f"instruments:symbol:{trading_symbol}", iid)
    if underlying_id is not None:
        conn.sadd(f"instruments:underlying_instrument_id:{underlying_id}", iid)
    if underlying_symbol:
        conn.sadd(f"instruments:underlying_trading_symbol:{underlying_symbol}", iid)
    if expiry_val:
        conn.sadd(f"instruments:expiry:{expiry_val.isoformat()}", iid)
    if getattr(model, "expired", False):
        conn.sadd("instruments:expired", iid)

# Clear existing instrument namespace then repopulate
_clear_instrument_namespace(redis_conn)

for inst in list_all_models:
    redis_conn.hset(f"instruments:{inst.instrument_id}", mapping=_inst_hash(inst))
    _add_indexes(redis_conn, inst)

# Option-specific helper: map option -> nearest future for Black76
list_fut_models = [m for m in list_all_models if (getattr(m, "segment", "").upper() == "NFO-FUT")]
list_opt_models = [m for m in list_all_models if (getattr(m, "segment", "").upper() == "NFO-OPT")]

def _fut_by_year_month(fut_models):
    buckets = {}
    for fut in fut_models:
        if not fut.expiry:
            continue
        key = (fut.expiry.year, fut.expiry.month)
        buckets.setdefault(key, []).append(fut)
    for key in buckets:
        buckets[key].sort(key=lambda f: f.expiry)
    return buckets

def _pick_fut_for_option(opt):
    if not opt.expiry or not list_fut_models:
        return None
    opt_underlying = (getattr(opt, "underlying_trading_symbol", "") or "").lower()
    fut_models_same_underlying = [
        fut
        for fut in list_fut_models
        if (getattr(fut, "underlying_trading_symbol", "") or "").lower() == opt_underlying
    ]
    if not fut_models_same_underlying:
        return None

    fut_lookup_same_underlying = _fut_by_year_month(fut_models_same_underlying)
    key = (opt.expiry.year, opt.expiry.month)
    if key in fut_lookup_same_underlying and fut_lookup_same_underlying[key]:
        return fut_lookup_same_underlying[key][0]
    try:
        return min(
            (f for f in fut_models_same_underlying if f.expiry),
            key=lambda f: abs((f.expiry - opt.expiry).days),
        )
    except ValueError:
        return None

for opt in list_opt_models:
    option_type = "call" if (opt.instrument_type or "").upper() in {"CE", "CALL", "C"} else "put"
    fut = _pick_fut_for_option(opt)
    mapping = {
        "option_type": option_type,
        "strike": str(opt.strike) if opt.strike is not None else "",
        "expiry": opt.expiry.isoformat() if opt.expiry is not None else "",
        "underlying_instrument_id": str(opt.underlying_instrument_id) if opt.underlying_instrument_id is not None else "",
        "underlying_future_instrument_id": str(fut.instrument_id) if fut is not None else "",
    }
    redis_conn.hset(f"instruments:opt:{opt.instrument_id}", mapping=mapping)

print(
    f"Loaded instruments into Redis: TOTAL={len(list_all_models)} OPT={len(list_opt_models)} FUT={len(list_fut_models)}"
)
logger.info(
    f"Loaded instruments into Redis: TOTAL={len(list_all_models)} OPT={len(list_opt_models)} FUT={len(list_fut_models)}"
)


# ===================================================================================================
# DONE

# close connections
pg_conn.close()
redis_conn.close()

sys.exit(1)
