"""
Broker Adapter Service
======================

Bridge service between Order Manager and Zerodha Kite broker, responsible for:
1. Consuming order commands from OMS and placing real broker orders
2. Listening to broker order updates via WebSocket and publishing to OMS
3. Translating between internal OMS format and broker-specific formats

This service decouples order management logic from broker-specific I/O, enabling:
- Broker-agnostic OMS design (can swap brokers without changing OMS)
- Testable OMS (can replay order updates for backtesting)
- Resilient architecture (OMS continues running if broker connection drops)

Architecture & Data Flow
------------------------
┌──────────────┐  WebSocket      ┌─────────────────┐  Redis Stream   ┌──────────┐
│ Kite Broker  │ ──────────────> │  This Service   │ ──────────────> │   OMS    │
│              │  order updates  │  (Translator)   │ oms:order_upd   │          │
└──────────────┘                 └─────────────────┘                 └──────────┘
       ↑                                  │
       │  REST API                        │  Redis Stream
       │  place_order()                   │  oms:commands
       └──────────────────────────────────┘

Input Sources
-------------
- **Command Stream**: Redis Stream `oms:commands` (OMS order placement requests)
- **Broker WebSocket**: Kite on_order_update callback (fill confirmations, rejections)
- **Access Token**: Redis key `kite:access_token` (refreshed by auth service)

Processing Pipeline
-------------------
**Command Processing** (oms:commands → Broker API):
1. **Consume**: XREADGROUP from oms:commands stream
2. **Translate**: Convert OMS order format to Kite API format
3. **Place Order**: Call kite.place_order() REST API
4. **Update OMS**: Publish broker_order_id back to oms:order_updates
5. **Acknowledge**: XACK command message

**Order Update Processing** (Broker WebSocket → oms:order_updates):
1. **Receive**: on_order_update callback invoked on background thread
2. **Bridge**: Schedule async coroutine on main loop (thread-safe)
3. **Translate**: Convert Kite order update to OMS format
4. **Publish**: XADD to oms:order_updates stream
5. **Metrics**: Update counters and latency histograms

Output Streams
--------------
**Order Updates Stream** (oms:order_updates):
- Published on every broker order state change
- Schema: order_id, broker_order_id, status, filled_qty, filled_price, timestamp
- Consumed by: Order Manager (03_order_manager.py)

Key Technical Details
--------------------
**Dual API Usage**:
- **REST API** (KiteConnect): place_order, cancel_order, modify_order
- **WebSocket** (KiteTicker): on_order_update callback for real-time fill updates

**Thread Safety**:
- KiteTicker runs on_order_update in separate thread
- Asyncio loop captured via get_running_loop()
- Updates scheduled via run_coroutine_threadsafe

**Order Translation**:
- OMS uses internal order_id (UUID)
- Broker returns broker_order_id (external reference)
- Both stored in oms:order:{order_id} hash for mapping

Prometheus Metrics
------------------
Exposed on port :9205 (configurable via BROKER_ADAPTER_METRICS_PORT)

- **broker_commands_read_total** (Counter): Order commands read from OMS
- **broker_commands_acked_total** (Counter): Order commands acknowledged
- **broker_orders_placed_total** (Counter): Orders successfully placed with broker
- **broker_orders_failed_total** (Counter): Order placement failures
- **broker_updates_received_total** (Counter): Order updates received from broker
- **broker_updates_published_total** (Counter): Order updates published to OMS
- **broker_connected** (Gauge): WebSocket connection status (1=up, 0=down)
- **broker_errors_total** (Counter, labeled by error_type): Errors encountered
- **broker_latency_seconds** (Histogram): Order placement latency

Configuration (Environment Variables)
-------------------------------------
- KITE_API_KEY: Zerodha API key for authentication
- REDIS_HOST, REDIS_PORT: Redis connection details
- BROKER_ADAPTER_GROUP: Consumer group name (default: broker_adapter_group)
- BROKER_ADAPTER_METRICS_PORT: Prometheus metrics server port (default: 9205)
- DIR_LOGS: Directory for log file output

Reliability & Observability
---------------------------
- Structured logging with timezone-aware timestamps (Asia/Kolkata)
- Log file: `artham_01_broker_adapter.log` (DEBUG level)
- Automatic reconnection managed by KiteTicker library
- Connection status monitoring via metrics
- Per-operation error tracking with labeled counters

Deployment Notes
----------------
- Requires active Kite access token (refreshed by separate auth service)
- Single instance recommended (avoid duplicate order placement)
- Works in live mode only (not for paper trading or backtesting)
- Docker container: exposes metrics port 9205, mounts logs volume
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Optional
from decouple import config
from redis.asyncio import Redis
from kiteconnect import KiteConnect, KiteTicker
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import logging
import os
from zoneinfo import ZoneInfo


# Configure custom timezone for logging
def time_converter(*args):
    """Convert log record time to configured timezone."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter

logging.basicConfig(
    filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_order_01_broker_adapter.log")),
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

GROUP_NAME = config("ORDER_BROKER_ADAPTER_GROUP", cast=str)
METRICS_PORT = config("ORDER_BROKER_ADAPTER_METRICS_PORT", cast=int)

# Stream keys
STREAM_ORDER_BROKER_COMMANDS = config("STREAM_ORDER_BROKER_COMMANDS", cast=str)
STREAM_ORDER_UPDATES = config("STREAM_ORDER_UPDATES", cast=str)
BROKER_ORDER_MAPPING = config("BROKER_ORDER_MAPPING", cast=str)

# -----------------------------
# Prometheus Metrics
# -----------------------------
BROKER_COMMANDS_READ_TOTAL = Counter(
    "broker_commands_read_total", "Total order commands read from OMS"
)
BROKER_COMMANDS_ACKED_TOTAL = Counter(
    "broker_commands_acked_total", "Total order commands acknowledged"
)
BROKER_ORDERS_PLACED_TOTAL = Counter(
    "broker_orders_placed_total", "Orders successfully placed with broker"
)
BROKER_ORDERS_FAILED_TOTAL = Counter(
    "broker_orders_failed_total", "Order placement failures"
)
BROKER_UPDATES_RECEIVED_TOTAL = Counter(
    "broker_updates_received_total", "Order updates received from broker"
)
BROKER_UPDATES_PUBLISHED_TOTAL = Counter(
    "broker_updates_published_total", "Order updates published to OMS"
)
BROKER_CONNECTED = Gauge(
    "broker_connected", "WebSocket connection status (1=up, 0=down)"
)
BROKER_ERRORS_TOTAL = Counter(
    "broker_errors_total", "Errors encountered", ["error_type"]
)
BROKER_LATENCY_SECONDS = Histogram(
    "broker_latency_seconds", "Order placement latency"
)


# -----------------------------
# Kite Broker WebSocket Adapter
# -----------------------------
class KITE_BROKER_WEBSOCKET:
    """KiteTicker adapter for order update callbacks.
    
    Listens to on_order_update callback and publishes updates to Redis.
    Runs on separate thread, bridges to asyncio via run_coroutine_threadsafe.
    """
    
    def __init__(self, api_key: str, access_token: str):
        self.api_key = api_key
        self.access_token = access_token
        self.loop = asyncio.get_running_loop()
        self.ws = KiteTicker(api_key, access_token)
        
        # Bind callbacks
        self.ws.on_order_update = self.on_order_update
        self.ws.on_connect = self.on_connect
        self.ws.on_close = self.on_close
        self.ws.on_error = self.on_error
        self.ws.on_reconnect = self.on_reconnect
        self.ws.on_noreconnect = self.on_noreconnect
    
    def start(self):
        """Start WebSocket connection in background thread."""
        self.ws.connect(threaded=True)
        logger.info("Kite order update WebSocket connecting...")
    
    def on_connect(self, ws, response):
        """Called when WebSocket connects."""
        logger.info("Kite order update WebSocket connected")
        BROKER_CONNECTED.set(1)
    
    def on_close(self, ws, code, reason):
        """Called when WebSocket closes."""
        logger.warning(f"Kite order update WebSocket closed: {code} {reason}")
        BROKER_CONNECTED.set(0)
    
    def on_error(self, ws, code, reason):
        """Called on WebSocket error."""
        logger.error(f"Kite order update WebSocket error: {code} {reason}")
        BROKER_CONNECTED.set(0)
    
    def on_reconnect(self, ws, attempt_count):
        """Called on reconnection attempt."""
        logger.warning(f"Kite order update WebSocket reconnect attempt {attempt_count}")
        BROKER_CONNECTED.set(0)
    
    def on_noreconnect(self, ws):
        """Called when reconnection fails permanently."""
        logger.error("Kite order update WebSocket gave up reconnecting")
        BROKER_CONNECTED.set(0)
    
    def on_order_update(self, ws, data):
        """Bridge threaded callback → asyncio (thread-safe).
        
        Called by KiteTicker when order state changes (placed, filled, cancelled, rejected).
        """
        try:
            asyncio.run_coroutine_threadsafe(
                self._publish_order_update_async(data), self.loop
            )
        except Exception as e:
            logger.exception(f"Failed to schedule order update publish: {e}")
    
    async def _publish_order_update_async(self, data: dict):
        """Publish order update to oms:order_updates stream."""
        try:
            BROKER_UPDATES_RECEIVED_TOTAL.inc()

            broker_order_id = str(data.get("order_id"))
            order_id = None
            try:
                order_id = await REDIS_CONN.hget(BROKER_ORDER_MAPPING, broker_order_id)
            except Exception:
                order_id = None
            
            # Translate Kite order update to OMS format
            update = {
                "order_id": order_id or "",
                "broker_order_id": broker_order_id,
                "status": data.get("status"),  # COMPLETE, OPEN, CANCELLED, REJECTED
                "filled_quantity": int(data.get("filled_quantity", 0)),
                "average_price": float(data.get("average_price", 0)),
                "pending_quantity": int(data.get("pending_quantity", 0)),
                "status_message": data.get("status_message", ""),
                "exchange_update_timestamp": data.get("exchange_update_timestamp", ""),
                "order_timestamp": data.get("order_timestamp", ""),
                "exchange_order_id": data.get("exchange_order_id", ""),
                "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
            }
            
            await REDIS_CONN.xadd(
                name=STREAM_ORDER_UPDATES,
                fields=normalize_for_redis(update),
                maxlen=100000,
                approximate=True,
            )
            
            BROKER_UPDATES_PUBLISHED_TOTAL.inc()
            logger.info(f"Published order update: broker_order_id={update['broker_order_id']}, status={update['status']}")
            
        except Exception as e:
            BROKER_ERRORS_TOTAL.labels(error_type="update_publish").inc()
            logger.exception(f"Failed to publish order update: {e}")


def normalize_for_redis(payload: dict) -> dict:
    """Normalize payload for Redis storage."""
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


# -----------------------------
# Order Placement Logic
# -----------------------------
async def place_broker_order(kite: KiteConnect, cmd: dict):
    """Translate OMS command to Kite API and place order."""
    try:
        t0 = time.perf_counter()
        
        # Extract OMS order details
        order_id = cmd.get("order_id")
        instrument_id = cmd.get("instrument_id")
        side = cmd.get("side")  # BUY or SELL
        qty = int(cmd.get("qty"))
        order_type = cmd.get("order_type")  # LIMIT, MARKET, SL, SL-M
        price = cmd.get("price")
        trigger_price = cmd.get("trigger_price")
        symbol = cmd.get("symbol")
        exchange = cmd.get("exchange", "NSE")

        print(f"DEBUG: price={price}, trigger_price={trigger_price}, symbol={symbol}, exchange={exchange}")
        
        # Translate to Kite format
        tradingsymbol = symbol
        
        # Map exchange to Kite exchange constant
        if exchange == "NSE":
            kite_exchange = kite.EXCHANGE_NSE
        elif exchange == "BSE":
            kite_exchange = kite.EXCHANGE_BSE
        elif exchange == "NFO":
            kite_exchange = kite.EXCHANGE_NFO
        elif exchange == "BFO":
            kite_exchange = kite.EXCHANGE_BFO
        elif exchange == "MCX":
            kite_exchange = kite.EXCHANGE_MCX
        elif exchange == "CDS":
            kite_exchange = kite.EXCHANGE_CDS
        else:
            kite_exchange = kite.EXCHANGE_NSE  # Default
        
        transaction_type = kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL
        
        # Map order types
        if order_type == "MARKET":
            kite_order_type = kite.ORDER_TYPE_MARKET
        elif order_type == "LIMIT":
            kite_order_type = kite.ORDER_TYPE_LIMIT
        elif order_type == "SL":
            kite_order_type = kite.ORDER_TYPE_SL
        elif order_type == "SL-M":
            kite_order_type = kite.ORDER_TYPE_SLM
        else:
            kite_order_type = kite.ORDER_TYPE_LIMIT
        
        # Place order via Kite REST API
        broker_order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite_exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transaction_type,
            quantity=qty,
            order_type=kite_order_type,
            price=float(price) if price else None,
            trigger_price=float(trigger_price) if trigger_price else None,
            product=kite.PRODUCT_MIS,  # Intraday
        )
        
        BROKER_ORDERS_PLACED_TOTAL.inc()
        BROKER_LATENCY_SECONDS.observe(time.perf_counter() - t0)
        
        # Store broker -> order mapping
        try:
            if broker_order_id:
                await REDIS_CONN.hset(BROKER_ORDER_MAPPING, str(broker_order_id), str(order_id))
        except Exception as e:
            logger.warning(f"Failed to store broker mapping: {e}")

        # Publish confirmation back to OMS
        confirmation = {
            "order_id": order_id,
            "broker_order_id": str(broker_order_id),
            "status": "PLACED",
            "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        }
        
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_UPDATES,
            fields=normalize_for_redis(confirmation),
            maxlen=100000,
            approximate=True,
        )
        
        logger.info(f"Placed order: order_id={order_id}, broker_order_id={broker_order_id}")
        
    except Exception as e:
        BROKER_ORDERS_FAILED_TOTAL.inc()
        BROKER_ERRORS_TOTAL.labels(error_type="order_placement").inc()
        logger.exception(f"Failed to place order: {e}")
        
        # Publish rejection to OMS
        rejection = {
            "order_id": cmd.get("order_id"),
            "broker_order_id": "",
            "status": "REJECTED",
            "status_message": str(e),
            "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        }
        
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_UPDATES,
            fields=normalize_for_redis(rejection),
            maxlen=100000,
            approximate=True,
        )


# -----------------------------
# Stream Workers
# -----------------------------
async def _get_broker_order_id(order_id: Optional[str], broker_order_id: Optional[str]) -> Optional[str]:
    if broker_order_id:
        return broker_order_id
    if not order_id:
        return None
    try:
        return await REDIS_CONN.hget(BROKER_ORDER_MAPPING, order_id)
    except Exception:
        return None


async def cancel_broker_order(kite: KiteConnect, cmd: dict):
    try:
        order_id = cmd.get("order_id")
        broker_order_id = await _get_broker_order_id(order_id, cmd.get("broker_order_id"))
        if not broker_order_id:
            logger.warning(f"CANCEL_ORDER missing broker_order_id for order_id={order_id}")
            return

        kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=broker_order_id)

        update = {
            "order_id": order_id or "",
            "broker_order_id": str(broker_order_id),
            "status": "CANCELLED",
            "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_UPDATES,
            fields=normalize_for_redis(update),
            maxlen=100000,
            approximate=True,
        )
        logger.info(f"Cancelled order: order_id={order_id}, broker_order_id={broker_order_id}")
    except Exception as e:
        BROKER_ERRORS_TOTAL.labels(error_type="order_cancel").inc()
        logger.exception(f"Failed to cancel order: {e}")


async def modify_broker_order(kite: KiteConnect, cmd: dict):
    try:
        order_id = cmd.get("order_id")
        broker_order_id = await _get_broker_order_id(order_id, cmd.get("broker_order_id"))
        if not broker_order_id:
            logger.warning(f"MODIFY_ORDER missing broker_order_id for order_id={order_id}")
            return

        price = cmd.get("price")
        trigger_price = cmd.get("trigger_price")
        qty = cmd.get("qty")

        kite.modify_order(
            variety=kite.VARIETY_REGULAR,
            order_id=broker_order_id,
            quantity=int(qty) if qty not in [None, ""] else None,
            price=float(price) if price not in [None, ""] else None,
            trigger_price=float(trigger_price) if trigger_price not in [None, ""] else None,
        )

        update = {
            "order_id": order_id or "",
            "broker_order_id": str(broker_order_id),
            "status": "MODIFIED",
            "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        }
        await REDIS_CONN.xadd(
            name=STREAM_ORDER_UPDATES,
            fields=normalize_for_redis(update),
            maxlen=100000,
            approximate=True,
        )
        logger.info(f"Modified order: order_id={order_id}, broker_order_id={broker_order_id}")
    except Exception as e:
        BROKER_ERRORS_TOTAL.labels(error_type="order_modify").inc()
        logger.exception(f"Failed to modify order: {e}")

async def process_commands(kite: KiteConnect):
    """Consume OMS commands and place broker orders."""
    logger.info("Command processor starting")
    
    streams = {STREAM_ORDER_BROKER_COMMANDS: ">"}
    
    while True:
        try:
            resp = await REDIS_CONN.xreadgroup(
                groupname=GROUP_NAME,
                consumername="broker_cmd_consumer",
                streams=streams,
                count=100,
                block=3000,
            )
            
            if not resp:
                continue
            
            for stream, entries in resp:
                BROKER_COMMANDS_READ_TOTAL.inc(len(entries))
                
                for msg_id, cmd in entries:
                    try:
                        command_type = cmd.get("command")
                        
                        # Only process order placement commands
                        if command_type == "PLACE_ORDER":
                            await place_broker_order(kite, cmd)
                        elif command_type == "CANCEL_ORDER":
                            await cancel_broker_order(kite, cmd)
                        elif command_type == "MODIFY_ORDER":
                            await modify_broker_order(kite, cmd)
                        
                    except Exception as e:
                        BROKER_ERRORS_TOTAL.labels(error_type="command_processing").inc()
                        logger.exception(f"Failed to process command: {e}")
                    
                    await REDIS_CONN.xack(stream, GROUP_NAME, msg_id)
                    BROKER_COMMANDS_ACKED_TOTAL.inc()
                    
        except Exception as e:
            BROKER_ERRORS_TOTAL.labels(error_type="command_loop").inc()
            logger.exception(f"Command processing loop error: {e}")
            await asyncio.sleep(1)


async def init_consumer_groups():
    """Initialize consumer groups for command stream."""
    try:
        await REDIS_CONN.xgroup_create(
            name=STREAM_ORDER_BROKER_COMMANDS,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception:
        pass  # BUSYGROUP likely


async def worker():
    """Main worker: initialize connections, start processors."""
    # Connectivity check
    try:
        if await REDIS_CONN.ping():
            print("[BROKER] Connected to Redis (PING ok)")
            logger.info("Connected to Redis (PING ok)")
    except Exception as e:
        print(f"[BROKER][ERROR] Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        return
    
    # Initialize Kite REST API client
    api_key = config("KITE_API_KEY", cast=str)
    access_token = await REDIS_CONN.get("kite:access_token")
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    logger.info("Kite REST API client initialized")
    
    # Initialize Kite WebSocket for order updates
    ws_handler = KITE_BROKER_WEBSOCKET(api_key, access_token)
    ws_handler.start()
    
    await init_consumer_groups()
    logger.info("Consumer groups initialized for broker adapter")
    
    print(f"[BROKER] Starting processor. group={GROUP_NAME}")
    logger.info(f"Starting processor. group={GROUP_NAME}")
    
    # Run command processor (WebSocket runs in background thread)
    await process_commands(kite)


if __name__ == "__main__":
    # Start Prometheus metrics server
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"[METRICS] Prometheus metrics server started on :{METRICS_PORT}")
    except Exception as e:
        logger.error(
            f"[METRICS][ERROR] Failed to start metrics server on :{METRICS_PORT}: {e}"
        )
        raise SystemExit(1)
    
    asyncio.run(worker())
