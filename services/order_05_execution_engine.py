"""
Order Execution Engine
======================

Evaluates ticks against active brackets and requests exits when targets or
stoploss levels are hit.
"""

import asyncio
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
    filename=os.path.join(config("DIR_LOGS", cast=str), "artham_order_05_execution_engine.log"),
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

GROUP_NAME = config("ORDER_EXECUTION_ENGINE_GROUP", cast=str)
METRICS_PORT = config("ORDER_EXECUTION_ENGINE_METRICS_PORT", cast=int)
PAPER_TRADING = config("ORDER_MANAGER_PAPER_TRADING", cast=bool)

STREAM_TICKS = config("STREAM_TICKS", cast=str)
STREAM_ORDER_STATE_COMMANDS = config("STREAM_ORDER_STATE_COMMANDS", cast=str)

TICKS_READ_TOTAL = Counter("execution_ticks_read_total", "Ticks read")
EXIT_REQUESTS_TOTAL = Counter("execution_exit_requests_total", "Exit requests published")
ENTRY_HITS_TOTAL = Counter("execution_entry_hits_total", "Entry hits published")
PROCESS_LATENCY_SECONDS = Histogram("execution_latency_seconds", "Tick processing latency")
REDIS_CONNECTED = Gauge("execution_redis_connected", "Redis connectivity")


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


async def publish_exit_hit(bracket_id: str, exit_type: str, filled_price: float):
    try:
        cmd = {
            "command": "EXIT_HIT",
            "bracket_id": bracket_id,
            "exit_type": exit_type,
            "filled_price": filled_price,
            "timestamp": now_iso(),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_STATE_COMMANDS,
            fields=normalize_for_redis(cmd),
            maxlen=100000,
            approximate=True,
        )
        EXIT_REQUESTS_TOTAL.inc()
    except Exception as e:
        logger.exception(f"Failed to publish exit hit: {e}")


async def publish_entry_hit(bracket_id: str, filled_price: float):
    try:
        cmd = {
            "command": "ENTRY_HIT",
            "bracket_id": bracket_id,
            "filled_price": filled_price,
            "timestamp": now_iso(),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_STATE_COMMANDS,
            fields=normalize_for_redis(cmd),
            maxlen=100000,
            approximate=True,
        )
        ENTRY_HITS_TOTAL.inc()
    except Exception as e:
        logger.exception(f"Failed to publish entry hit: {e}")


async def evaluate_exits_for_tick(instrument_id: str, price: float):
    try:
        bracket_ids = await REDIS_CONN.smembers(f"oms:active:instrument:{instrument_id}")
        if not bracket_ids:
            return

        for bracket_id in bracket_ids:
            bracket = await REDIS_CONN.hgetall(f"oms:bracket:{bracket_id}")
            if not bracket:
                continue

            state = bracket.get("state")
            if state == "ENTRY_PLACED" and PAPER_TRADING:
                side = bracket.get("side")
                entry_price = float(bracket.get("entry_price", 0))
                entry_hit = False
                if side == "BUY":
                    entry_hit = price <= entry_price
                elif side == "SELL":
                    entry_hit = price >= entry_price

                if entry_hit:
                    logger.info(f"Entry hit for bracket {bracket_id} at price {price}")
                    await publish_entry_hit(bracket_id, price)
                continue

            if state != "EXIT_ORDERS_PLACED":
                continue

            side = bracket.get("side")
            target_price = float(bracket.get("target_price", 0))
            stoploss_price = float(bracket.get("stoploss_price", 0))

            target_hit = False
            stoploss_hit = False

            if side == "BUY":
                target_hit = price >= target_price
                stoploss_hit = price <= stoploss_price
            elif side == "SELL":
                target_hit = price <= target_price
                stoploss_hit = price >= stoploss_price

            if target_hit:
                logger.info(f"Target hit for bracket {bracket_id} at price {price}")
                await publish_exit_hit(bracket_id, "target", price)
            elif stoploss_hit:
                logger.info(f"Stoploss hit for bracket {bracket_id} at price {price}")
                await publish_exit_hit(bracket_id, "stoploss", price)

    except Exception as e:
        logger.exception(f"Failed to evaluate exits for {instrument_id}: {e}")


async def process_ticks():
    logger.info("Execution engine tick processor starting")
    streams = {STREAM_TICKS: ">"}

    while True:
        try:
            resp = await REDIS_CONN.xreadgroup(
                groupname=GROUP_NAME,
                consumername="execution_tick_consumer",
                streams=streams,
                count=500,
                block=3000,
            )

            if not resp:
                continue

            for stream, entries in resp:
                TICKS_READ_TOTAL.inc(len(entries))

                for msg_id, tick in entries:
                    t0 = time.perf_counter()
                    try:
                        instrument_id = str(tick.get("instrument_id"))
                        price_val = tick.get("last_price") or tick.get("price")
                        if price_val is None:
                            await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
                            continue
                        price = float(price_val)
                        await evaluate_exits_for_tick(instrument_id, price)
                        # logger.debug(f"Processed tick for {instrument_id} at price {price}")
                    except Exception as e:
                        logger.exception(f"Failed to process tick: {e}")

                    await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
                    PROCESS_LATENCY_SECONDS.observe(time.perf_counter() - t0)

        except Exception as e:
            logger.exception(f"Execution engine loop error: {e}")
            await asyncio.sleep(1)


async def init_consumer_groups():
    try:
        await REDIS_CONN.xgroup_create(
            name=STREAM_TICKS,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception:
        pass


async def worker():
    try:
        if await REDIS_CONN.ping():
            print("[EXECUTION] Connected to Redis")
            logger.info("Connected to Redis")
            REDIS_CONNECTED.set(1)
    except Exception as e:
        print(f"[EXECUTION][ERROR] Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        REDIS_CONNECTED.set(0)
        return

    await init_consumer_groups()
    logger.info("Consumer groups initialized")

    print(f"[EXECUTION] Starting processor. group={GROUP_NAME}")
    logger.info(f"Starting processor. group={GROUP_NAME}")

    await process_ticks()


if __name__ == "__main__":
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on :{METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        raise SystemExit(1)

    asyncio.run(worker())
