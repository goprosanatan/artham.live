"""
Equity Depth Feature Service

Consumes ticks from Redis Stream `md:ticks`, extracts buy/sell depth
levels, computes quantity/order ratios per price, and publishes
normalized depth snapshots for Reliance equity instruments.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from decouple import config
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis


def time_converter(*args):
	return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter

logging.basicConfig(
	filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_01_feature_equity.log")),
	encoding="utf-8",
	level=logging.DEBUG,
	datefmt="%Y-%m-%d %H:%M:%S %p %Z",
	format="%(asctime)s %(levelname)-8s %(message)s",
)

logger.debug("Equity depth feature service starting up")


STREAM_TICKS = config("STREAM_TICKS", cast=str)
STREAM_FEATURE_EQUITY = config("STREAM_FEATURE_EQUITY", cast=str)

GROUP_NAME = config("FEATURE_EQUITY_GROUP", cast=str)
BATCH_SIZE = config("FEATURE_EQUITY_BATCH_SIZE", cast=int)
METRICS_PORT = config("FEATURE_EQUITY_METRICS_PORT", cast=int)


REDIS_CONN = Redis(
	host=config("REDIS_HOST", cast=str),
	port=config("REDIS_PORT", cast=int),
	decode_responses=True,
)


FEATURE_EQUITY_READ_TOTAL = Counter(
	"feature_equity_read_total", "Total tick messages read for equity depth"
)
FEATURE_EQUITY_PUBLISH_TOTAL = Counter(
	"feature_equity_publish_total", "Total equity depth snapshots published"
)
FEATURE_EQUITY_ERRORS_TOTAL = Counter(
	"feature_equity_errors_total", "Total errors in equity depth service"
)
FEATURE_EQUITY_REDIS_CONNECTED = Gauge(
	"feature_equity_redis_connected", "Redis connectivity status (1=up, 0=down)"
)
FEATURE_EQUITY_PROCESS_DURATION_SECONDS = Histogram(
	"feature_equity_process_duration_seconds",
	"Equity depth processing latency in seconds",
)


def _to_float(val) -> Optional[float]:
	try:
		if val is None:
			return None
		return float(val)
	except Exception:
		return None


def _to_int(val) -> Optional[int]:
	try:
		if val is None:
			return None
		return int(float(val))
	except Exception:
		return None


def _build_depth_map(
	tick: dict,
	side: str,
	last_price: Optional[float],
) -> Dict[str, Dict[str, Optional[float]]]:
	levels: Dict[str, Dict[str, Optional[float]]] = {}
	for i in range(5):
		price = _to_float(tick.get(f"depth_{side}_{i}_price"))
		qty = _to_int(tick.get(f"depth_{side}_{i}_quantity"))
		orders = _to_int(tick.get(f"depth_{side}_{i}_orders"))

		if price is None or qty is None or orders is None:
			continue

		if last_price is not None:
			if side == "buy" and price > last_price:
				continue
			if side == "sell" and price < last_price:
				continue

		ratio = (qty / orders) if orders else None
		levels[str(price)] = {
			"price": price,
			"quantity": qty,
			"orders": orders,
			"ratio": ratio,
		}
	return levels


async def init_consumer_groups():
	try:
		await REDIS_CONN.xgroup_create(
			name=STREAM_TICKS,
			groupname=GROUP_NAME,
			id="0",
			mkstream=True,
		)
		logger.info("Consumer group created for equity depth feature")
	except Exception:
		pass


async def worker():
	try:
		if await REDIS_CONN.ping():
			FEATURE_EQUITY_REDIS_CONNECTED.set(1)
			logger.info("Connected to Redis (PING ok)")
	except Exception as exc:
		FEATURE_EQUITY_REDIS_CONNECTED.set(0)
		logger.error("Redis connection failed: %s", exc)
		return

	reliance_eq = {
		int(t)
		for t in await REDIS_CONN.sinter(
			"instruments:type:eq", "instruments:symbol:reliance"
		)
	}
	logger.info("Loaded Reliance EQ instruments: %s", len(reliance_eq))

	await init_consumer_groups()
	streams = {STREAM_TICKS: ">"}

	while True:
		resp = await REDIS_CONN.xreadgroup(
			groupname=GROUP_NAME,
			consumername="feature_equity_consumer",
			streams=streams,
			count=BATCH_SIZE,
			block=5000,
		)

		if not resp:
			continue

		for stream, messages in resp:
			for msg_id, values in messages:
				FEATURE_EQUITY_READ_TOTAL.inc()
				t0 = time.perf_counter()
				try:
					tick = dict(values)
					instrument_id = _to_int(tick.get("instrument_id"))
					instrument_type = (tick.get("instrument_type") or "").upper()

					if instrument_id is None or instrument_type != "EQ":
						await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
						continue
					if reliance_eq and instrument_id not in reliance_eq:
						await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
						continue

					last_price = _to_float(tick.get("last_price"))
					if last_price is None or last_price <= 0:
						await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
						continue

					buy_levels = _build_depth_map(tick, "buy", last_price)
					sell_levels = _build_depth_map(tick, "sell", last_price)

					payload = {
						"instrument_id": str(instrument_id),
						"last_price": str(last_price),
						"exchange_ts": str(tick.get("exchange_ts") or ""),
						"ingest_ts": str(tick.get("ingest_ts") or ""),
						"buy_levels": json.dumps(buy_levels, separators=(",", ":")),
						"sell_levels": json.dumps(sell_levels, separators=(",", ":")),
					}

					await REDIS_CONN.xadd(
						name=STREAM_FEATURE_EQUITY,
						fields=payload,
						maxlen=500000,
						approximate=True,
					)

					FEATURE_EQUITY_PUBLISH_TOTAL.inc()
					await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
				except Exception as exc:
					FEATURE_EQUITY_ERRORS_TOTAL.inc()
					logger.exception("Failed processing message %s: %s", msg_id, exc)
					await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
				finally:
					FEATURE_EQUITY_PROCESS_DURATION_SECONDS.observe(
						time.perf_counter() - t0
					)


if __name__ == "__main__":
	try:
		start_http_server(METRICS_PORT)
		logger.info("[METRICS] Prometheus server started on :%s", METRICS_PORT)
	except Exception as exc:
		logger.error("[METRICS][ERROR] Failed to start metrics server: %s", exc)
		raise SystemExit(1)

	asyncio.run(worker())
