"""
Risk Manager Service
====================

Evaluates risk limits and approves or rejects order commands.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from decouple import config
from redis.asyncio import Redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from zoneinfo import ZoneInfo


def time_converter(*args):
    return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter

logging.basicConfig(
    filename=os.path.join(config("DIR_LOGS", cast=str), "artham_order_03_risk_manager.log"),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

REDIS_CONN = Redis(
    host=config("REDIS_HOST", cast=str),
    port=config("REDIS_PORT", cast=int),
    decode_responses=True,
)

GROUP_NAME = config("ORDER_RISK_MANAGER_GROUP", cast=str)
METRICS_PORT = config("ORDER_RISK_MANAGER_METRICS_PORT", cast=int)

STREAM_ORDER_RISK_REQUESTS = config("STREAM_ORDER_RISK_REQUESTS", cast=str)
STREAM_ORDER_STATE_COMMANDS = config("STREAM_ORDER_STATE_COMMANDS", cast=str)
STREAM_ORDER_COMMAND_RESPONSES = config("STREAM_ORDER_COMMAND_RESPONSES", cast=str)

MAX_NOTIONAL = config("RISK_MAX_NOTIONAL", default=1000000, cast=float)

RISK_READ_TOTAL = Counter("risk_read_total", "Risk requests read")
RISK_APPROVED_TOTAL = Counter("risk_approved_total", "Risk requests approved")
RISK_REJECTED_TOTAL = Counter("risk_rejected_total", "Risk requests rejected")
RISK_LATENCY_SECONDS = Histogram("risk_latency_seconds", "Risk processing latency")
REDIS_CONNECTED = Gauge("risk_redis_connected", "Redis connectivity")


def now_iso() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()


def normalize_for_redis(payload: dict) -> dict:
    out = {}
    for k, v in payload.items():
        if v is None:
            continue
        elif isinstance(v, bool):
            out[k] = int(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def validate_risk(cmd: dict) -> str:
    command = (cmd.get("command") or "").upper()
    if command != "PLACE_BRACKET":
        return ""

    try:
        qty = int(cmd.get("qty"))
        entry_price = float(cmd.get("entry_price"))
        target_price = float(cmd.get("target_price"))
        stoploss_price = float(cmd.get("stoploss_price"))
    except Exception:
        return "Invalid numeric fields"

    if qty <= 0 or entry_price <= 0 or target_price <= 0 or stoploss_price <= 0:
        return "Prices and qty must be > 0"

    side = (cmd.get("side") or "").upper()
    if side == "BUY":
        if not (target_price > entry_price > stoploss_price):
            return "Price order invalid for BUY"
    elif side == "SELL":
        if not (target_price < entry_price < stoploss_price):
            return "Price order invalid for SELL"
    else:
        return "Invalid side"

    notional = qty * entry_price
    if notional > MAX_NOTIONAL:
        return "Notional limit exceeded"

    return ""


async def send_response(request_id: str, success: bool, message: str, data: dict = None):
    if not request_id:
        return
    try:
        response = {
            "request_id": request_id,
            "success": int(success),
            "message": message,
            "timestamp": now_iso(),
            "data": json.dumps(data or {}),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_COMMAND_RESPONSES,
            fields=normalize_for_redis(response),
            maxlen=50000,
            approximate=True,
        )
    except Exception as e:
        logger.exception(f"Failed to send response: {e}")


async def process_risk_requests():
    logger.info("Risk manager processor starting")
    streams = {STREAM_ORDER_RISK_REQUESTS: ">"}

    while True:
        try:
            resp = await REDIS_CONN.xreadgroup(
                groupname=GROUP_NAME,
                consumername="risk_consumer",
                streams=streams,
                count=200,
                block=3000,
            )

            if not resp:
                continue

            for stream, entries in resp:
                RISK_READ_TOTAL.inc(len(entries))

                for msg_id, cmd in entries:
                    t0 = time.perf_counter()
                    request_id = cmd.get("request_id")

                    try:
                        error = validate_risk(cmd)
                        if error:
                            RISK_REJECTED_TOTAL.inc()
                            await send_response(request_id, False, error)
                        else:
                            await REDIS_CONN.xadd(
                                name=STREAM_ORDER_STATE_COMMANDS,
                                fields=normalize_for_redis(cmd),
                                maxlen=100000,
                                approximate=True,
                            )
                            RISK_APPROVED_TOTAL.inc()
                    except Exception as e:
                        logger.exception(f"Failed to process risk request: {e}")

                    await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
                    RISK_LATENCY_SECONDS.observe(time.perf_counter() - t0)

        except Exception as e:
            logger.exception(f"Risk manager loop error: {e}")
            await asyncio.sleep(1)


async def init_consumer_groups():
    try:
        await REDIS_CONN.xgroup_create(
            name=STREAM_ORDER_RISK_REQUESTS,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception:
        pass


async def worker():
    try:
        if await REDIS_CONN.ping():
            print("[RISK] Connected to Redis")
            logger.info("Connected to Redis")
            REDIS_CONNECTED.set(1)
    except Exception as e:
        print(f"[RISK][ERROR] Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        REDIS_CONNECTED.set(0)
        return

    await init_consumer_groups()
    logger.info("Consumer groups initialized")

    print(f"[RISK] Starting processor. group={GROUP_NAME}")
    logger.info(f"Starting processor. group={GROUP_NAME}")

    await process_risk_requests()


if __name__ == "__main__":
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on :{METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        raise SystemExit(1)

    asyncio.run(worker())
