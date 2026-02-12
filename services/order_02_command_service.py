"""
Order Command Service
=====================

Validates API commands and forwards approved commands to Risk Manager.
"""

import asyncio
import json
import logging
import os
import time
import uuid
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
    filename=os.path.join(config("DIR_LOGS", cast=str), "artham_order_02_command_service.log"),
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

GROUP_NAME = config("ORDER_COMMAND_SERVICE_GROUP", cast=str)
METRICS_PORT = config("ORDER_COMMAND_SERVICE_METRICS_PORT", cast=int)

STREAM_ORDER_API_COMMANDS = config("STREAM_ORDER_API_COMMANDS", cast=str)
STREAM_ORDER_RISK_REQUESTS = config("STREAM_ORDER_RISK_REQUESTS", cast=str)
STREAM_ORDER_COMMAND_RESPONSES = config("STREAM_ORDER_COMMAND_RESPONSES", cast=str)

COMMANDS_READ_TOTAL = Counter("order_command_read_total", "Commands read")
COMMANDS_FORWARDED_TOTAL = Counter("order_command_forwarded_total", "Commands forwarded")
COMMANDS_REJECTED_TOTAL = Counter("order_command_rejected_total", "Commands rejected")
COMMAND_LATENCY_SECONDS = Histogram("order_command_latency_seconds", "Command processing latency")
REDIS_CONNECTED = Gauge("order_command_redis_connected", "Redis connectivity")


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


def validate_command(cmd: dict) -> str:
    command = (cmd.get("command") or "").upper()
    if not command:
        return "Missing command"

    if command == "PLACE_BRACKET":
        required_fields = [
            "strategy_id",
            "instrument_id",
            "side",
            "qty",
            "entry_price",
            "target_price",
            "stoploss_price",
        ]
        for field in required_fields:
            if cmd.get(field) in [None, ""]:
                return f"Missing field: {field}"
        side = (cmd.get("side") or "").upper()
        if side not in ["BUY", "SELL"]:
            return "side must be BUY or SELL"
        try:
            qty = int(cmd.get("qty"))
            if qty <= 0:
                return "qty must be > 0"
        except Exception:
            return "qty must be an integer"
    elif command == "CANCEL_BRACKET":
        if not cmd.get("bracket_id"):
            return "Missing field: bracket_id"
    elif command == "MODIFY_SL_TP":
        if not cmd.get("bracket_id"):
            return "Missing field: bracket_id"
        if cmd.get("target_price") in [None, ""] and cmd.get("stoploss_price") in [None, ""]:
            return "Provide target_price or stoploss_price"
    elif command == "FORCE_EXIT":
        if not cmd.get("bracket_id"):
            return "Missing field: bracket_id"
    else:
        return f"Unknown command: {command}"

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


async def process_commands():
    logger.info("Command service processor starting")
    streams = {STREAM_ORDER_API_COMMANDS: ">"}

    while True:
        try:
            resp = await REDIS_CONN.xreadgroup(
                groupname=GROUP_NAME,
                consumername="order_command_consumer",
                streams=streams,
                count=200,
                block=3000,
            )

            if not resp:
                continue

            for stream, entries in resp:
                COMMANDS_READ_TOTAL.inc(len(entries))

                for msg_id, cmd in entries:
                    t0 = time.perf_counter()
                    request_id = cmd.get("request_id") or str(uuid.uuid4())
                    cmd["request_id"] = request_id

                    try:
                        error = validate_command(cmd)
                        if error:
                            COMMANDS_REJECTED_TOTAL.inc()
                            await send_response(request_id, False, error)
                        else:
                            await REDIS_CONN.xadd(
                                name=STREAM_ORDER_RISK_REQUESTS,
                                fields=normalize_for_redis(cmd),
                                maxlen=100000,
                                approximate=True,
                            )
                            COMMANDS_FORWARDED_TOTAL.inc()
                    except Exception as e:
                        logger.exception(f"Failed to process command: {e}")

                    await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
                    COMMAND_LATENCY_SECONDS.observe(time.perf_counter() - t0)

        except Exception as e:
            logger.exception(f"Command service loop error: {e}")
            await asyncio.sleep(1)


async def init_consumer_groups():
    try:
        await REDIS_CONN.xgroup_create(
            name=STREAM_ORDER_API_COMMANDS,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception:
        pass


async def worker():
    try:
        if await REDIS_CONN.ping():
            print("[ORDER_COMMAND] Connected to Redis")
            logger.info("Connected to Redis")
            REDIS_CONNECTED.set(1)
    except Exception as e:
        print(f"[ORDER_COMMAND][ERROR] Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        REDIS_CONNECTED.set(0)
        return

    await init_consumer_groups()
    logger.info("Consumer groups initialized")

    print(f"[ORDER_COMMAND] Starting processor. group={GROUP_NAME}")
    logger.info(f"Starting processor. group={GROUP_NAME}")

    await process_commands()


if __name__ == "__main__":
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on :{METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        raise SystemExit(1)

    asyncio.run(worker())
