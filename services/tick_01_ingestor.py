"""
Market Data Ingestor Service
============================

Real-time market data ingestion service that connects to Zerodha Kite WebSocket feed, 
receives live market ticks for configured instruments, normalizes and enriches them, 
and publishes to Redis Streams for consumption by downstream processing services.

Architecture & Data Flow
------------------------
┌──────────────┐     WebSocket      ┌─────────────────┐      Redis Stream        ┌────────────────┐
│ Kite Ticker  │ ─────────────────> │  This Service   │ ───────────────────────> │  Tick Store    │
│ (Zerodha)    │   MODE_FULL ticks  │  (Normalizer)   │   md:ticks (XADD)        │  Feature Eng   │
└──────────────┘                    └─────────────────┘                          │  Bar Builder   │
                                            │                                    └────────────────┘
                                            │
                                    ┌───────┴────────┐
                                    │   Redis Cache  │
                                    │ - access_token │
                                    │ - instruments  │
                                    └────────────────┘

Input Sources
-------------
- **WebSocket Feed**: KiteTicker WebSocket connection (threaded client)
- **Instrument Universe**: Redis Sets (pre-populated)
- **Access Token**: Redis key `kite:access_token` (refreshed by auth service)

Processing Pipeline
-------------------
1. **Subscribe**: On WebSocket connect, subscribe to instrument_ids with MODE_FULL
2. **Receive**: KiteTicker invokes `on_ticks` callback on background thread
3. **Bridge**: Thread-safe scheduling to main asyncio loop via `run_coroutine_threadsafe`
4. **Flatten**: Convert nested tick dict to flat structure using `flatten_json`
5. **Normalize**: Transform booleans to ints, datetimes to ISO format for Redis
6. **Enrich**: Add `instrument_id`, localized timestamps (`exchange_ts`, `ingest_ts`)
7. **Publish**: XADD to Redis Stream `md:ticks` with 1M message cap (approximate)

Output Schema
-------------
Published tick fields (flattened):
- instrument_id, instrument_token, tradable, mode
- last_price, last_quantity, average_price, volume, buy_quantity, sell_quantity
- ohlc (open, high, low, close)
- change, last_trade_time, oi (open interest), oi_day_high, oi_day_low
- depth (buy/sell orders with quantity, price, orders)
- exchange_ts, ingest_ts (ISO 8601 with Asia/Kolkata timezone)

Key Technical Details
--------------------
**Thread Safety**: 
- KiteTicker runs in separate thread (threaded=True)
- Asyncio loop captured via `get_running_loop()` during initialization
- Tick publishing scheduled via `run_coroutine_threadsafe` for safe cross-thread communication

**Reconnection Handling**:
- Automatic reconnection managed by KiteTicker library
- Connection state exposed via Prometheus gauge `ingestor_connected`
- Callbacks: on_reconnect, on_noreconnect for monitoring

**Error Handling**:
- Per-tick exception catching with counter metrics
- Partial batch success (one tick failure doesn't block others)
- Detailed exception logging with stack traces

Prometheus Metrics
------------------
Exposed on port :9201 (configurable via TICK_INGESTOR_METRICS_PORT)

- **ingestor_ticks_total** (Counter): Total ticks received from Kite WebSocket
- **ingestor_publish_total** (Counter): Total ticks successfully published to Redis
- **ingestor_errors_total** (Counter): Total errors during tick processing/publish
- **ingestor_latency_seconds** (Histogram): Time between tick receipt and Redis publish
- **ingestor_connected** (Gauge): WebSocket connection status (1=connected, 0=disconnected)

Configuration (Environment Variables)
-------------------------------------
- KITE_API_KEY: Zerodha API key for WebSocket authentication
- REDIS_HOST, REDIS_PORT: Redis connection details
- TICK_INGESTOR_METRICS_PORT: Prometheus metrics server port (default: 9201)
- DIR_LOGS: Directory for log file output
- Session timezone: Asia/Kolkata (hardcoded for IST market hours)

Reliability & Observability
---------------------------
- Structured logging with timezone-aware timestamps (Asia/Kolkata)
- Log file: `artham_tick_01_ingestor.log` (DEBUG level)
- Graceful shutdown support via asyncio.CancelledError
- Connection status monitoring via metrics and logs
- Real-time console output for batch ingestion confirmation

Performance Characteristics
---------------------------
- Non-blocking async Redis I/O (redis.asyncio)
- Batch processing of tick arrays from Kite
- Stream capped at ~1M messages (approximate, Redis XADD MAXLEN)
- Typical latency: <50ms from tick receipt to Redis publish

Deployment Notes
----------------
- Requires active Kite access token (refreshed by separate auth service)
- Instrument list must be pre-populated in Redis Set
- Single instance recommended (WebSocket connection limit per API key)
- Docker container: exposes metrics port, mounts logs volume
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import os
import psycopg
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis
from decouple import config
from kiteconnect import KiteTicker
from flatten_json import flatten
from zoneinfo import ZoneInfo


# Configure custom timezone for logging
# This ensures all log timestamps are in configured timezone regardless of system timezone.
def time_converter(*args):
    """Convert log record time to configured timezone."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter

logging.basicConfig(
    filename=(
        os.path.join(config("DIR_LOGS", cast=str), "artham_tick_01_ingestor.log")
    ),
    encoding="utf-8",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S %p %Z",
    format="%(asctime)s %(levelname)-8s %(message)s",
)

logger.debug("Tick 01 Ingestor starting up")

# -----------------------------------------------------------------------------
# Clients
# -----------------------------------------------------------------------------
# Async Redis client used for publishing ticks to Redis Streams.
# decode_responses=True ensures values are str (not bytes) when reading.
REDIS_CONN = Redis(
    host=config("REDIS_HOST", cast=str),
    port=config("REDIS_PORT", cast=int),
    # password=config("REDIS_PASSWORD", cast=str),
    decode_responses=True,
)


STREAM_TICKS = config("STREAM_TICKS", cast=str)

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
METRICS_PORT = config("TICK_INGESTOR_METRICS_PORT", cast=int)
# Prometheus metrics (kept label-free for simple dashboards/alerts).
# Exposed via start_http_server(METRICS_PORT) in main().
INGESTOR_TICKS_TOTAL = Counter(
    "ingestor_ticks_total",
    "Total ticks received from Kite",
)
INGESTOR_PUBLISH_TOTAL = Counter(
    "ingestor_publish_total",
    "Total ticks published to Redis Streams",
)
INGESTOR_ERRORS_TOTAL = Counter(
    "ingestor_errors_total",
    "Total errors while publishing ticks to Redis Streams",
)
INGESTOR_LATENCY_SECONDS = Histogram(
    "ingestor_latency_seconds",
    "Latency between tick receipt and publish",
    buckets=[0.01, 0.05, 0.1, 0.5, 1],
)
INGESTOR_CONNECTED = Gauge(
    "ingestor_connected",
    "Feed websocket connection status (1=connected, 0=disconnected)",
)


# -----------------------------------------------------------------------------
# Kite Connect WebSocket Adapter (threaded client → async bridge)
# -----------------------------------------------------------------------------

logger.debug("Initializing Kite WebSocket handler")


class KITE_WEBSOCKET:
    """Threaded KiteTicker adapter that publishes ticks into Redis Streams.

    - KiteTicker uses an internal thread and invokes callbacks on that thread.
    - We capture the main asyncio loop via asyncio.get_running_loop() during
        construction (must be called from within an active event loop context).
    - on_ticks schedules an async coroutine on the main loop using
        asyncio.run_coroutine_threadsafe so that Redis I/O is done asynchronously.
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        instruments_eq: set[int],
        instruments_opt: set[int],
        instruments_fut: set[int],
    ):
        if KiteTicker is None:
            raise RuntimeError("kiteconnect is not installed. pip install kiteconnect")

        self.api_key = api_key
        self.access_token = access_token
        self.instruments = instruments
        self.instruments_eq = instruments_eq
        self.instruments_opt = instruments_opt
        self.instruments_fut = instruments_fut
        # Capture the currently running asyncio loop so we can schedule coroutines
        # from KiteTicker's background thread safely. Must be called inside a
        # running loop (see main()).
        self.loop = asyncio.get_running_loop()
        # Underlying threaded websocket client provided by Kite Connect.
        self.ws = KiteTicker(api_key, access_token)

        # Bind callbacks
        self.ws.on_ticks = self.on_ticks
        self.ws.on_connect = self.on_connect
        self.ws.on_close = self.on_close
        self.ws.on_error = self.on_error

        # Reconnection strategy
        self.ws.on_reconnect = self.on_reconnect
        self.ws.on_noreconnect = self.on_noreconnect

    def start(self):
        """Start the KiteTicker client in a background thread.

        This returns immediately; callbacks begin once the connection is
        established. Subscription is performed in on_connect.
        """
        # Start websocket in its own thread; our code remains on the asyncio loop.
        self.ws.connect(threaded=True)
        logger.info("KiteTicker connecting...")

    # ----------------------------- Callbacks ---------------------------------
    def on_connect(self, ws, response):
        """Called by KiteTicker when the WebSocket is connected.

        Subscribes to the configured list of instrument_ids and requests
        MODE_FULL to receive full tick depth (LTP, depth, OI, etc.).
        """
        logger.info("KiteTicker connected, subscribing to instruments")
        INGESTOR_CONNECTED.set(1)
        if self.instruments:
            # Subscribe to tick streams for our instrument token list.
            ws.subscribe(self.instruments)
            # Request full tick depth (includes bid/ask depth, OI, etc.).
            ws.set_mode(ws.MODE_FULL, self.instruments)

    def on_close(self, ws, code, reason):
        """Called when the WebSocket closes for any reason."""
        logger.warning(f"KiteTicker closed: {code} {reason}")
        INGESTOR_CONNECTED.set(0)

    def on_error(self, ws, code, reason):
        """Called when the WebSocket reports an error from the server/client."""
        logger.error(f"KiteTicker error: {code} {reason}")
        INGESTOR_CONNECTED.set(0)

    def on_reconnect(self, ws, attempt_count):
        """Called when KiteTicker attempts to reconnect after a drop."""
        logger.warning(f"KiteTicker reconnect attempt {attempt_count}")
        # transient state: keep at 0 until on_connect
        INGESTOR_CONNECTED.set(0)

    def on_noreconnect(self, ws):
        """Called when KiteTicker stops trying to reconnect (max retries hit)."""
        logger.error("KiteTicker gave up reconnecting")
        INGESTOR_CONNECTED.set(0)

    def on_ticks(self, ws, ticks):
        """Bridge threaded callback → asyncio (thread-safe).

        KiteTicker invokes this on its own thread. We must not perform blocking
        operations (like network I/O) directly here. Instead we schedule the
        async publisher coroutine on the main loop.
        """
        # KiteTicker invokes this in its own thread; schedule coroutine on main loop
        try:
            # Submit work to the main asyncio loop; returns a Future which we
            # don't await here (threaded context). Exceptions are logged below.
            asyncio.run_coroutine_threadsafe(
                self._publish_ticks_async(ticks), self.loop
            )
        except Exception as e:
            logger.exception(f"Failed to schedule tick publish: {e}")

    async def _publish_ticks_async(self, ticks):
        """Flatten and publish ticks to Redis Stream STREAM_KEY via XADD.
        Normalizes bool→int, datetime→isoformat, and leaves other types as is.
        """
        success_count = 0
        error_count = 0
        tz = ZoneInfo("Asia/Kolkata")
        
        for tick in ticks:
            try:
                payload = flatten(tick)
                payload["instrument_id"] = payload.get("instrument_token")
                payload["instrument_type"] = self._instrument_type(payload.get("instrument_token"))
                
                # Attach timezone to naive exchange timestamps from Kite
                # (already in IST, just need timezone label for consistency)
                exchange_ts = payload.get("exchange_timestamp")
                if exchange_ts and exchange_ts.tzinfo is None:
                    payload["exchange_ts"] = exchange_ts.replace(tzinfo=tz)
                else:
                    payload["exchange_ts"] = exchange_ts
                
                last_trade_time = payload.get("last_trade_time")
                if last_trade_time and last_trade_time.tzinfo is None:
                    payload["last_trade_time"] = last_trade_time.replace(tzinfo=tz)
                else:
                    payload["last_trade_time"] = last_trade_time
                
                # Get current time in IST (timezone-aware)
                payload["ingest_ts"] = datetime.now(tz)

                payload = normalize_for_redis(payload)

                INGESTOR_TICKS_TOTAL.inc()

                t0 = datetime.now()
                await REDIS_CONN.xadd(
                    name=STREAM_TICKS,
                    fields=payload,
                    maxlen=1000000,
                    approximate=True,
                )
                INGESTOR_PUBLISH_TOTAL.inc()
                INGESTOR_LATENCY_SECONDS.observe((datetime.now() - t0).total_seconds())
                success_count += 1
            except Exception as e:
                INGESTOR_ERRORS_TOTAL.inc()
                error_count += 1
                logger.exception(f"Failed to publish tick: {e}")
        if success_count or error_count:
            summary = f"[INGESTED INTO REDIS STREAMS] {success_count} ticks"
            if error_count:
                summary += f" | {error_count} errors"

            # Emit at error level if any failures, otherwise at info.
            (logger.error if error_count else logger.info)(summary)
            print(summary)

    def _instrument_type(self, instrument_id: Optional[int]) -> str:
        if instrument_id in self.instruments_eq:
            return "EQ"
        if instrument_id in self.instruments_opt:
            return "OPT"
        if instrument_id in self.instruments_fut:
            return "FUT"
        return "UNKNOWN"


# -----------------------------------------------------------------------------
# Helper: Normalize tick for Redis (convert bool→int, datetime→isoformat)
# -----------------------------------------------------------------------------
def normalize_for_redis(payload: dict) -> dict:
    out = {}
    for k, v in payload.items():
        if isinstance(v, bool):
            out[k] = int(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
async def worker():
    """Entrypoint: wires env/config, starts KiteTicker, and keeps loop alive.

    Notes:
    - `access_token` is currently hard-coded for local testing. In production,
        prefer: `access_token = config("KITE_ACCESS_TOKEN", cast=str)`.
    - This process runs forever, sleeping the event loop, while KiteTicker
        handles the socket in a background thread.
    - If you want to log lack of connectivity or stalled ticks, add a periodic
        monitor task that inspects last received tick time and connection events.
    """

    logger.info("Tick 01 Ingestor starting main()")

    # Read API credentials and instrument universe.
    api_key = config("KITE_API_KEY", cast=str)

    # Access token is stored in Redis by a separate login flow.
    access_token = await REDIS_CONN.get("kite:access_token")

    # Resolve instrument_ids from the new instrument universe for Reliance
    instruments_eq = {
        int(t)
        for t in await REDIS_CONN.sinter(
            "instruments:type:eq", "instruments:symbol:reliance"
        )
    }
    instruments_opt = {
        int(t)
        for t in await REDIS_CONN.sinter(
            "instruments:segment:nfo-opt", "instruments:underlying_trading_symbol:reliance"
        )
    }
    instruments_fut = {
        int(t)
        for t in await REDIS_CONN.sinter(
            "instruments:segment:nfo-fut", "instruments:underlying_trading_symbol:reliance"
        )
    }
    instruments_all = set().union(instruments_eq, instruments_opt, instruments_fut)

    logger.info(
        "Loaded Reliance instruments eq=%s opt=%s fut=%s all=%s",
        len(instruments_eq),
        len(instruments_opt),
        len(instruments_fut),
        len(instruments_all),
    )

    # Create the threaded websocket adapter and initiate the connection.
    handler = KITE_WEBSOCKET(
        api_key=api_key,
        access_token=access_token,
        instruments=list(instruments_all),
        instruments_eq=instruments_eq,
        instruments_opt=instruments_opt,
        instruments_fut=instruments_fut,
    )
    handler.start()

    logger.info("Kite WebSocket handler started")

    # Keep the asyncio loop alive while KiteTicker runs in its own thread
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Feed handler shutting down")


if __name__ == "__main__":

    # Start Prometheus metrics server (fail fast if bind fails)
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"[TICK_01_INGESTOR] Prometheus metrics server started on :{METRICS_PORT}")
    except Exception as e:
        logger.error(
            f"[TICK_01_INGESTOR][ERROR] Failed to start metrics server on :{METRICS_PORT}: {e}"
        )
        raise SystemExit(1)

    asyncio.run(worker())
