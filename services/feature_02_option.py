"""
Option Engine Service

Consumes ticks from the market data ingestor (Redis Stream `md:ticks`), selects the
appropriate pricing model per instrument, solves implied volatility, computes Greeks,
and publishes enriched option features to Redis for downstream feature and signal
pipelines.
"""

import asyncio
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Optional
from numbers import Number, Integral

from decouple import config
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis
from zoneinfo import ZoneInfo

from library.core.option import EXPIRY, BLACK_76


# Configure custom timezone for logging
def time_converter(*args):
   return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter

logging.basicConfig(
   filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_feature_02_option.log")),
   encoding="utf-8",
   level=logging.DEBUG,
   datefmt="%Y-%m-%d %H:%M:%S %p %Z",
   format="%(asctime)s %(levelname)-8s %(message)s",
)

logger.debug("Feature Option starting up")


# Streams and config
STREAM_TICKS = config("STREAM_TICKS", cast=str)
STREAM_FEATURE_OPTIONS = config("STREAM_FEATURE_OPTIONS", cast=str)

GROUP_NAME = config("FEATURE_OPTION_GROUP", cast=str)
BATCH_SIZE = config("FEATURE_OPTION_BATCH_SIZE", cast=int)
METRICS_PORT = config("FEATURE_OPTION_METRICS_PORT", cast=int)

RISK_FREE_RATE = config("FEATURE_OPTION_RISK_FREE_INTEREST_RATE", cast=float)
DEFAULT_OPTION_IV = config("FEATURE_OPTION_DEFAULT_IV", cast=float)


# Redis connection
REDIS_CONN = Redis(
   host=config("REDIS_HOST", cast=str),
   port=config("REDIS_PORT", cast=int),
   decode_responses=True,
)


# Prometheus metrics
OPTIONENGINE_READ_TOTAL = Counter(
   "optionengine_read_total", "Total tick messages read for option engine"
)
OPTIONENGINE_PROCESSED_TOTAL = Counter(
   "optionengine_processed_total", "Total option ticks processed"
)
OPTIONENGINE_PUBLISH_TOTAL = Counter(
   "optionengine_publish_total", "Total option feature messages published"
)
OPTIONENGINE_ERRORS_TOTAL = Counter(
   "optionengine_errors_total", "Total errors encountered in option engine"
)
OPTIONENGINE_REDIS_CONNECTED = Gauge(
   "optionengine_redis_connected", "Redis connectivity status (1=up, 0=down)"
)
OPTIONENGINE_PROCESS_DURATION_SECONDS = Histogram(
   "optionengine_process_duration_seconds", "Processing time per tick"
)


# Data structures
@dataclass
class OptionMeta:
   instrument_id: int
   option_type: str  # "call" or "put"
   strike: float
   expiry: date
   underlying_instrument_id: Optional[int]  # equity id (reference)
   underlying_future_instrument_id: Optional[int]  # futures id for Black76 pricing


def normalize_for_redis(payload: dict) -> dict:
   out: dict = {}
   for k, v in payload.items():
      if v is None:
         continue
      if isinstance(v, bool):
         out[k] = int(v)
      elif isinstance(v, (datetime, date)):
         out[k] = v.isoformat()
      elif isinstance(v, Decimal):
         out[k] = float(v)
      elif isinstance(v, Integral):
         # Preserve integer identifiers (instrument_id, underlying ids) as ints
         # so Redis keys align with websocket subscription keys.
         out[k] = int(v)
      elif isinstance(v, Number):
         # Normalize numpy scalar numerics (e.g., np.float64) to native Python
         # so Redis stores plain numeric strings instead of repr like np.float64(...)
         out[k] = float(v)
      else:
         out[k] = v
   return out


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
      return int(val)
   except Exception:
      return None


def _to_datetime(val) -> Optional[datetime]:
   if not val:
      return None
   try:
      return datetime.fromisoformat(val)
   except Exception:
      return None


class OptionEngine:
   def __init__(self, redis_conn: Redis):
      self.redis = redis_conn
      self.expiry_helper = EXPIRY()
      self.option_meta: Dict[int, OptionMeta] = {}
      self.price_cache: Dict[int, float] = {}
      self.future_candidates_cache: Dict[str, list] = {}

   async def _get_future_candidates(self, underlying_symbol: str) -> list:
      symbol = (underlying_symbol or "").strip().lower()
      if not symbol:
         return []
      if symbol in self.future_candidates_cache:
         return self.future_candidates_cache[symbol]

      future_ids = await self.redis.sinter(
         "instruments:segment:nfo-fut",
         f"instruments:underlying_trading_symbol:{symbol}",
      )

      candidates = []
      for fid_raw in future_ids:
         fid = _to_int(fid_raw)
         if fid is None:
            continue
         fut_hash = await self.redis.hgetall(f"instruments:{fid}")
         if not fut_hash:
            continue
         expiry_str = fut_hash.get("expiry")
         expiry_val = None
         if expiry_str:
            try:
               expiry_val = datetime.fromisoformat(expiry_str).date()
            except Exception:
               expiry_val = None
         candidates.append((fid, expiry_val))

      self.future_candidates_cache[symbol] = candidates
      return candidates

   @staticmethod
   def _pick_future_id_for_option(expiry_val: date, candidates: list) -> Optional[int]:
      if not candidates:
         return None
      with_expiry = [(fid, exp) for fid, exp in candidates if exp is not None]
      if not with_expiry:
         return None

      same_month = [
         (fid, exp) for fid, exp in with_expiry if exp.year == expiry_val.year and exp.month == expiry_val.month
      ]
      if same_month:
         same_month.sort(key=lambda row: row[1])
         return same_month[0][0]

      with_expiry.sort(key=lambda row: abs((row[1] - expiry_val).days))
      return with_expiry[0][0]

   async def load_option_metadata(self):
      # Reliance universe via new instrument index sets
      option_ids = {
         int(t)
         for t in await self.redis.sinter(
            "instruments:segment:nfo-opt", "instruments:underlying_trading_symbol:reliance"
         )
      }
      if not option_ids:
         logger.warning(
            "No option instruments found for Reliance via instruments:segment:nfo-opt ∩ instruments:underlying_trading_symbol:reliance"
         )
         return

      meta: Dict[int, OptionMeta] = {}
      for oid in option_ids:
         base_data = await self.redis.hgetall(f"instruments:{oid}")
         data = await self.redis.hgetall(f"instruments:opt:{oid}")
         if not data:
            logger.debug("Missing option hash for %s", oid)
            continue

         strike = _to_float(data.get("strike"))
         expiry_str = data.get("expiry") or ""
         expiry_val: Optional[date] = None
         if expiry_str:
            try:
               expiry_val = datetime.fromisoformat(expiry_str).date()
            except Exception:
               logger.debug("Invalid expiry for option %s: %s", oid, expiry_str)

         if strike is None or expiry_val is None:
            continue

         option_raw = (data.get("option_type") or "").lower()
         option_type = "call" if option_raw in {"call", "c", "ce"} else "put"

         underlying_id = _to_int(data.get("underlying_instrument_id"))
         underlying_fut_id = _to_int(data.get("underlying_future_instrument_id"))
         underlying_symbol = (
            data.get("underlying_trading_symbol")
            or base_data.get("underlying_trading_symbol")
            or ""
         )
         fut_candidates = await self._get_future_candidates(underlying_symbol)
         fut_candidate_ids = {fid for fid, _ in fut_candidates}

         if underlying_fut_id and underlying_fut_id not in fut_candidate_ids:
            fallback_fut_id = self._pick_future_id_for_option(expiry_val, fut_candidates)
            if fallback_fut_id is not None:
               logger.warning(
                  "Option %s had mismatched future id %s for underlying %s; corrected to %s",
                  oid,
                  underlying_fut_id,
                  (underlying_symbol or "").upper(),
                  fallback_fut_id,
               )
               underlying_fut_id = fallback_fut_id
            else:
               logger.debug(
                  "Underlying future %s for option %s not in futures set; will likely miss price",
                  underlying_fut_id,
                  oid,
               )
         elif underlying_fut_id is None:
            fallback_fut_id = self._pick_future_id_for_option(expiry_val, fut_candidates)
            if fallback_fut_id is not None:
               underlying_fut_id = fallback_fut_id

         meta[int(oid)] = OptionMeta(
            instrument_id=int(oid),
            option_type=option_type,
            strike=strike,
            expiry=expiry_val,
            underlying_instrument_id=underlying_id,
            underlying_future_instrument_id=underlying_fut_id,
         )

      self.option_meta = meta
      logger.info("Loaded option metadata for %s instruments", len(meta))

   def _update_price_cache(self, instrument_id: Optional[int], tick: dict):
      if instrument_id is None:
         return
      # Only keep prices for instruments marked as FUT to avoid polluting forward cache
      instrument_type = (tick.get("instrument_type") or "").upper()
      if instrument_type and instrument_type != "FUT":
         return

      price = _to_float(tick.get("last_price"))
      if price is not None:
         self.price_cache[instrument_id] = price

   def _resolve_underlying_price(self, tick: dict, meta: OptionMeta) -> Optional[float]:
      # Prefer explicitly supplied underlying value (if publisher adds it)
      underlying_from_tick = _to_float(tick.get("underlying_value"))
      if underlying_from_tick is not None:
         return underlying_from_tick

      # Use futures price for Black76
      if meta.underlying_future_instrument_id is None:
         return None

      return self.price_cache.get(meta.underlying_future_instrument_id)

   def _time_to_expiry_days(self, expiry_dt: date) -> Optional[float]:
      try:
         return float(self.expiry_helper.get_days_to_expiry(expiry_dt))
      except Exception as exc:
         logger.debug("Failed to compute days_to_expiry for %s: %s", expiry_dt, exc)
         return None

   def _build_feature(self, tick: dict, meta: OptionMeta) -> Optional[dict]:
      market_price = _to_float(tick.get("last_price"))
      if market_price is None or market_price <= 0:
         return None

      underlying_price = self._resolve_underlying_price(tick, meta)
      if underlying_price is None or underlying_price <= 0:
         return None

      t_days = self._time_to_expiry_days(meta.expiry)
      if t_days is None or t_days <= 0:
         return None

      model = BLACK_76(F=underlying_price, K=meta.strike, r=RISK_FREE_RATE, t_days=t_days)

      sigma = model.implied_vol(
         market_price=market_price,
         option_type=meta.option_type,
         tol=1e-3,
         max_iter=200,
      )

      if not math.isfinite(sigma) or sigma <= 0:
         sigma = DEFAULT_OPTION_IV

      greeks = model.greeks_scaled(sigma, option_type=meta.option_type)
      theo_price = (
         model.call_price(sigma)
         if meta.option_type == "call"
         else model.put_price(sigma)
      )

      if sigma is not None:
         sigma = sigma * 100.0  # convert to percentage

      feature = {
         "instrument_id": meta.instrument_id,
         "underlying_instrument_id": meta.underlying_instrument_id,
         "underlying_future_instrument_id": meta.underlying_future_instrument_id,
         "option_type": meta.option_type,
         "strike": meta.strike,
         "expiry": meta.expiry,
         "t_days": t_days,
         "underlying_price": underlying_price,
         "option_price": market_price,
         "implied_vol": sigma,
         "theoretical_price": theo_price,
         "delta": greeks.get("delta"),
         "gamma": greeks.get("gamma"),
         "vega": greeks.get("vega"),
         "theta": greeks.get("theta"),
         "rho": greeks.get("rho"),
         "exchange_ts": _to_datetime(tick.get("exchange_ts")),
         "ingest_ts": _to_datetime(tick.get("ingest_ts")),
      }

      return normalize_for_redis(feature)

   async def process_tick(self, tick: dict) -> Optional[dict]:
      instrument_id = _to_int(tick.get("instrument_id"))
      self._update_price_cache(instrument_id, tick)

      if instrument_id is None:
         return None

      meta = self.option_meta.get(instrument_id)
      if not meta:
         return None

      return self._build_feature(tick, meta)


async def init_consumer_groups():
   try:
      await REDIS_CONN.xgroup_create(
         name=STREAM_TICKS,
         groupname=GROUP_NAME,
         id="0",
         mkstream=True,
      )
      logger.info("Consumer group created for option engine")
   except Exception:
      pass


async def worker():
   # connectivity check
   try:
      if await REDIS_CONN.ping():
         OPTIONENGINE_REDIS_CONNECTED.set(1)
         logger.info("Connected to Redis (PING ok)")
   except Exception as exc:
      OPTIONENGINE_REDIS_CONNECTED.set(0)
      logger.error("Redis connection failed: %s", exc)
      return

   engine = OptionEngine(redis_conn=REDIS_CONN)
   await engine.load_option_metadata()

   await init_consumer_groups()
   streams = {STREAM_TICKS: ">"}
   logger.info(
      "Feature Option ready. group=%s batch_size=%s stream=%s -> %s",
      GROUP_NAME,
      BATCH_SIZE,
      STREAM_TICKS,
      STREAM_FEATURE_OPTIONS,
   )
   print("[OPTION_ENGINE] Ready. Listening for ticks.")

   while True:
      resp = await REDIS_CONN.xreadgroup(
         groupname=GROUP_NAME,
         consumername="option_engine_consumer",
         streams=streams,
         count=BATCH_SIZE,
         block=5000,
      )

      if not resp:
         continue

      for stream, messages in resp:
         for msg_id, values in messages:
            tick = dict(values)
            OPTIONENGINE_READ_TOTAL.inc()
            t0 = datetime.now()
            try:
               feature = await engine.process_tick(tick)
               if feature:
                  await REDIS_CONN.xadd(
                     name=STREAM_FEATURE_OPTIONS,
                     fields=feature,
                     maxlen=500000,
                     approximate=True,
                  )
                  OPTIONENGINE_PUBLISH_TOTAL.inc()
                  OPTIONENGINE_PROCESSED_TOTAL.inc()
                  logger.debug(feature)
               # logger.debug("Acking message %s (published=%s)", msg_id, bool(feature))
               await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
            except Exception as exc:
               OPTIONENGINE_ERRORS_TOTAL.inc()
               logger.exception("Failed processing message %s: %s", msg_id, exc)
               await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
            finally:
               OPTIONENGINE_PROCESS_DURATION_SECONDS.observe(
                  (datetime.now() - t0).total_seconds()
               )


if __name__ == "__main__":
   try:
      start_http_server(METRICS_PORT)
      logger.info("[METRICS] Prometheus server started on :%s", METRICS_PORT)
   except Exception as exc:
      logger.error("[METRICS][ERROR] Failed to start metrics server: %s", exc)
      raise SystemExit(1)

   asyncio.run(worker())
