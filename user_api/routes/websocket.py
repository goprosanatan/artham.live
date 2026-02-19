import socketio
import redis.asyncio as redis
import logging
import asyncio
import json
import re
from typing import Dict
from decouple import config

logger = logging.getLogger(__name__)

from library.modules import misc
from . import auth


# ==============================================================================
# DEFAULT Config - Websocket

# Create a Socket.IO server
websocket = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=[],
    # Disable verbose engine.io logs (packets, pings) to keep console clean
    logger=False,
    engineio_logger=False,
)

# Background task handle for fanout service
_fanout_task = None

NP_FLOAT_WRAPPER_RE = re.compile(r"^np\.float\d+\(([-+0-9.eE]+)\)$")


def _parse_float(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        text = val.strip()
        if not text:
            return None
        match = NP_FLOAT_WRAPPER_RE.match(text)
        if match:
            text = match.group(1)
        try:
            return float(text)
        except Exception:
            return None
    try:
        return float(val)
    except Exception:
        return None


async def startup_handler():
    """Start the websocket fanout service on app startup."""
    global _fanout_task
    logger.info("Starting websocket fanout service...")
    _fanout_task = asyncio.create_task(
        websocket_fanout_service(asgi_app.state.async_redis_pool)
    )


async def shutdown_handler():
    """Stop the websocket fanout service on app shutdown."""
    global _fanout_task
    if _fanout_task:
        logger.info("Stopping websocket fanout service...")
        _fanout_task.cancel()
        try:
            await _fanout_task
        except asyncio.CancelledError:
            logger.info("Websocket fanout service stopped")


asgi_app = socketio.ASGIApp(
    socketio_server=websocket,
    # other_asgi_app=app,
    socketio_path="",
    on_startup=startup_handler,
    on_shutdown=shutdown_handler,
)


# ==============================================================================
# Subscription Management (Redis-based)

# Redis keys:
# - ws:user.{sid}.instruments -> Set of instrument_id subscribed by this user
# - ws:instrument.{instrument_id}.users -> Set of user SIDs subscribed to this instrument
# - ws:active_instruments -> Set of all active instruments being streamed


async def subscription_start(sid: str, async_redis_pool):
    """Initialize subscription tracking for a new WebSocket connection."""
    async with redis.Redis(connection_pool=async_redis_pool) as redis_conn:
        # Clean up any stale subscription data from previous session
        await redis_conn.delete(f"ws:user.{sid}.instruments")
        logger.info(f"Subscription started for user: {sid}")


async def subscription_end(sid: str, async_redis_pool):
    """Clean up all subscriptions when user disconnects."""
    async with redis.Redis(connection_pool=async_redis_pool) as redis_conn:
        pipe = redis_conn.pipeline()
        total_cleaned = 0
        
        # Clean up subscriptions for each subscription type
        for sub_type in STREAM_SUBSCRIPTIONS.keys():
            # Get all instruments this user was subscribed to for this type
            user_instruments = await redis_conn.smembers(f"ws:user.{sid}.{sub_type}.instruments")
            
            # Remove user from each instrument's subscriber set
            for instrument_id in user_instruments:
                pipe.srem(f"ws:instrument.{sub_type}.{instrument_id}.users", sid)
            
            # Delete user's instrument set for this type
            pipe.delete(f"ws:user.{sid}.{sub_type}.instruments")
            total_cleaned += len(user_instruments)
        
        await pipe.execute()
        logger.info(f"Subscription ended for user: {sid}, cleaned up {total_cleaned} total subscriptions")


def replay_stream_key(session_id: str, sub_type: str) -> str:
    if sub_type == "bars.1m":
        return REPLAY_STREAM_TEMPLATE_1M.format(session_id=session_id)
    if sub_type == "bars.1D":
        return REPLAY_STREAM_TEMPLATE_1D.format(session_id=session_id)
    raise ValueError(f"Unsupported replay subscription type: {sub_type}")


def _replay_sub_task_key(sid: str, sub_type: str, session_id: str):
    return (sid, sub_type, session_id)


async def _stop_replay_task(sid: str, sub_type: str, session_id: str):
    task_key = _replay_sub_task_key(sid, sub_type, session_id)
    task = REPLAY_SUB_TASKS.pop(task_key, None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _stop_all_replay_tasks_for_sid(sid: str):
    task_keys = [key for key in REPLAY_SUB_TASKS if key[0] == sid]
    for key in task_keys:
        task = REPLAY_SUB_TASKS.pop(key, None)
        if not task:
            continue
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _replay_stream_reader(sid: str, sub_type: str, session_id: str, async_redis_pool):
    stream_key = replay_stream_key(session_id=session_id, sub_type=sub_type)
    last_id = "0"  # Start from the beginning to catch all historical bars for replay
    redis_conn = redis.Redis(connection_pool=async_redis_pool)

    try:
        while True:
            messages = await redis_conn.xread(
                streams={stream_key: last_id},
                count=100,
                block=1000,
            )

            if not messages:
                continue

            for _, entries in messages:
                for msg_id, values in entries:
                    payload = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in values.items()
                    }
                    await websocket.emit(
                        "bar",
                        {
                            "type": sub_type,
                            "mode": "replay",
                            "session_id": session_id,
                            "data": payload,
                        },
                        room=sid,
                    )
                    logger.info(
                        "[FANOUT][REPLAY] event=bar type=%s session_id=%s sid=%s msg_id=%s",
                        sub_type,
                        session_id,
                        sid,
                        msg_id,
                    )
                    last_id = msg_id
    except asyncio.CancelledError:
        logger.info(
            f"Replay stream reader cancelled: sid={sid}, type={sub_type}, session={session_id}"
        )
        raise
    except Exception as e:
        logger.exception(
            f"Replay stream reader failed: sid={sid}, type={sub_type}, session={session_id}, error={e}"
        )
    finally:
        await redis_conn.close()

async def subscription_add(sid: str, sub_type: str, instruments: list, async_redis_pool):
    """
    Subscribe user to instruments for a specific subscription type.
    
    Args:
        sid: Socket.IO session ID
        sub_type: Subscription type (e.g., 'bars.1m', 'bars.1D')
        instruments: List of instruments (integers)
        async_redis_pool: Redis connection pool
    """
    async with redis.Redis(connection_pool=async_redis_pool) as redis_conn:
        if not instruments:
            return
            
        pipe = redis_conn.pipeline()
        
        for instrument_id in instruments:
            instrument_id_str = str(instrument_id)
            # Add instrument to user's typed subscription set
            pipe.sadd(f"ws:user.{sid}.{sub_type}.instruments", instrument_id_str)
            # Add user to instrument's typed subscriber set
            pipe.sadd(f"ws:instrument.{sub_type}.{instrument_id_str}.users", sid)
            # Mark this instrument as active for this type
            pipe.sadd(f"ws:active_instruments.{sub_type}", instrument_id_str)
        
        await pipe.execute()
        logger.info(f"User {sid} subscribed to {len(instruments)} instruments for {sub_type}: {instruments}")


async def subscription_remove(sid: str, sub_type: str, instruments: list, async_redis_pool):
    """
    Unsubscribe user from instruments for a specific subscription type.
    
    Args:
        sid: Socket.IO session ID
        sub_type: Subscription type (e.g., 'bars.1m', 'bars.1D')
        instruments: List of instruments (integers)
        async_redis_pool: Redis connection pool
    """
    async with redis.Redis(connection_pool=async_redis_pool) as redis_conn:
        if not instruments:
            return
            
        pipe = redis_conn.pipeline()
        
        for instrument_id in instruments:
            instrument_id_str = str(instrument_id)
            # Remove instrument from user's typed subscription set
            pipe.srem(f"ws:user.{sid}.{sub_type}.instruments", instrument_id_str)
            # Remove user from instrument's typed subscriber set
            pipe.srem(f"ws:instrument.{sub_type}.{instrument_id_str}.users", sid)
        
        await pipe.execute()
        logger.info(f"User {sid} unsubscribed from {len(instruments)} instruments for {sub_type}: {instruments}")


# ==============================================================================
# WebSocket Fanout Service (Reads from Feature Engine Redis Streams)

FANOUT_CONSUMER_GROUP = config("WEBSOCKET_FANOUT_CONSUMER_GROUP", cast=str)
FANOUT_CONSUMER_NAME = "ws_fanout_consumer"

# Bar streams for typed subscriptions
BAR_STREAMS = {
    "bars.1m": "md:bars.live.1m",
    "bars.1D": "md:bars.live.1D",
}

# Feature streams for typed subscriptions
FEATURE_STREAMS = {
    "feature.equity_depth": config(
        "STREAM_FEATURE_EQUITY", cast=str, default="md:feature:equity"
    ),
    "feature.option_chain": config(
        "STREAM_FEATURE_OPTIONS", cast=str, default="md:feature:options"
    ),
}

STREAM_SUBSCRIPTIONS = {**BAR_STREAMS, **FEATURE_STREAMS}

REPLAY_BAR_TYPES = {"bars.1m", "bars.1D"}
REPLAY_STREAM_PREFIX = config("REPLAY_STREAM_PREFIX", cast=str, default="replay")
REPLAY_STREAM_TEMPLATE_1M = config(
    "WEBSOCKET_REPLAY_STREAM_1M_TEMPLATE",
    cast=str,
    default="replay:{session_id}:bar:1m",
)
REPLAY_STREAM_TEMPLATE_1D = config(
    "WEBSOCKET_REPLAY_STREAM_1D_TEMPLATE",
    cast=str,
    default="replay:{session_id}:bar:1D",
)

# Per-client replay fanout tasks keyed by (sid, sub_type, session_id)
REPLAY_SUB_TASKS = {}

# Order events fanout
ORDER_EVENTS_STREAM = config("OMS_EVENTS_STREAM", cast=str, default="oms:events")
ORDER_EVENTS_CONSUMER_GROUP = config(
    "WEBSOCKET_ORDER_EVENTS_CONSUMER_GROUP", cast=str, default="ws_order_events_group"
)
ORDER_EVENTS_CONSUMER_NAME = "ws_order_events_consumer"
ORDER_EVENT_TYPES = {
    "BRACKET_CREATED",
    "ENTRY_PLACED",
    "ENTRY_FILLED",
    "EXIT_ORDERS_PLACED",
    "TARGET_PLACED",
    "STOPLOSS_PLACED",
    "TARGET_FILLED",
    "STOPLOSS_FILLED",
    "EXIT_CANCELLED",
    "BRACKET_CANCELLED",
    "BRACKET_REJECTED",
    "BRACKET_COMPLETED",
    "FORCE_EXIT",
    "ORDER_REJECTED",
}



async def init_consumer_groups(redis_conn):
    """Create consumer groups for all subscribed streams if they don't exist."""
    for sub_type, stream_key in STREAM_SUBSCRIPTIONS.items():
        try:
            await redis_conn.xgroup_create(
                stream_key,
                FANOUT_CONSUMER_GROUP,
                id="$",  # Start from latest messages
                mkstream=True
            )
            logger.debug(f"Created consumer group for {sub_type} stream: {stream_key}")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Failed to create consumer group for {stream_key}: {e}")


async def cleanup_inactive_instruments(redis_conn):
    """Remove instruments from active set that have no subscribers."""
    for sub_type in STREAM_SUBSCRIPTIONS.keys():
        active_instruments = await redis_conn.smembers(f"ws:active_instruments.{sub_type}")
        
        for instrument_id in active_instruments:
            subscribers = await redis_conn.scard(f"ws:instrument.{sub_type}.{instrument_id}.users")
            if subscribers == 0:
                await redis_conn.srem(f"ws:active_instruments.{sub_type}", instrument_id)
                logger.debug(f"Removed inactive instrument {instrument_id} from {sub_type} active set")


async def websocket_fanout_service(async_redis_pool):
    """
    Main fanout service that:
    1. Reads from bar live streams (1m and 1D)
    2. Routes bars to subscribed WebSocket clients based on instrument_ids and subscription type
    """
    logger.info("[FANOUT] WebSocket fanout service started")

    async def stream_loop():
        redis_conn = redis.Redis(connection_pool=async_redis_pool)
        await init_consumer_groups(redis_conn)
        streams_dict = {stream_key: ">" for stream_key in STREAM_SUBSCRIPTIONS.values()}

        while True:
            try:
                await cleanup_inactive_instruments(redis_conn)

                resp = await redis_conn.xreadgroup(
                    groupname=FANOUT_CONSUMER_GROUP,
                    consumername=FANOUT_CONSUMER_NAME,
                    streams=streams_dict,
                    count=100,
                    block=1000,
                )

                if not resp:
                    continue

                for stream_key, messages in resp:
                    sub_type = next(
                        (st for st, sk in STREAM_SUBSCRIPTIONS.items() if sk == stream_key),
                        None,
                    )
                    if not sub_type:
                        logger.warning(f"Unknown stream key: {stream_key}")
                        continue

                    for msg_id, values in messages:
                        try:
                            payload_data = {}
                            for key, value in values.items():
                                k = key.decode() if isinstance(key, bytes) else key
                                v = value.decode() if isinstance(value, bytes) else value
                                payload_data[k] = v

                            instrument_id_raw = payload_data.get("instrument_id")
                            if not instrument_id_raw or instrument_id_raw == "None":
                                logger.warning(f"Message {msg_id} has invalid instrument_id: {instrument_id_raw}, skipping")
                                await redis_conn.xack(stream_key, FANOUT_CONSUMER_GROUP, msg_id)
                                continue

                            instrument_id_int = None
                            try:
                                instrument_id_int = int(float(instrument_id_raw))
                            except Exception:
                                instrument_id_int = None

                            instrument_id = (
                                str(instrument_id_int)
                                if instrument_id_int is not None
                                else str(instrument_id_raw)
                            )
                            subscribers = await redis_conn.smembers(f"ws:instrument.{sub_type}.{instrument_id}.users")

                            if subscribers:
                                if sub_type.startswith("bars."):
                                    for sid in subscribers:
                                        try:
                                            await websocket.emit(
                                                "bar",
                                                {"type": sub_type, "data": payload_data},
                                                room=sid,
                                            )
                                        except Exception as e:
                                            logger.error(f"Failed to emit to {sid}: {e}")

                                    logger.info(
                                        "[FANOUT][LIVE] event=bar type=%s instrument_id=%s recipients=%s msg_id=%s",
                                        sub_type,
                                        instrument_id,
                                        len(subscribers),
                                        msg_id,
                                    )
                                elif sub_type == "feature.equity_depth":
                                    buy_levels_raw = payload_data.get("buy_levels") or "{}"
                                    sell_levels_raw = payload_data.get("sell_levels") or "{}"
                                    try:
                                        buy_levels_map = json.loads(buy_levels_raw)
                                    except Exception:
                                        buy_levels_map = {}
                                    try:
                                        sell_levels_map = json.loads(sell_levels_raw)
                                    except Exception:
                                        sell_levels_map = {}

                                    def _to_int(val):
                                        try:
                                            return int(float(val))
                                        except Exception:
                                            return None

                                    def _levels_to_list(levels_map, side):
                                        levels = []
                                        for _, level in levels_map.items():
                                            price = _parse_float(level.get("price"))
                                            qty = _to_int(level.get("quantity"))
                                            orders = _to_int(level.get("orders"))
                                            ratio = _parse_float(level.get("ratio"))
                                            if price is None or qty is None:
                                                continue
                                            levels.append(
                                                {
                                                    "price": price,
                                                    "quantity": qty,
                                                    "orders": orders,
                                                    "ratio": ratio,
                                                }
                                            )
                                        reverse = side == "buy"
                                        levels.sort(key=lambda x: x["price"], reverse=reverse)
                                        return levels

                                    depth_payload = {
                                        "instrument_id": instrument_id,
                                        "last_price": _parse_float(payload_data.get("last_price")),
                                        "exchange_ts": payload_data.get("exchange_ts"),
                                        "ingest_ts": payload_data.get("ingest_ts"),
                                        "buy_levels": _levels_to_list(buy_levels_map, "buy"),
                                        "sell_levels": _levels_to_list(sell_levels_map, "sell"),
                                    }

                                    for sid in subscribers:
                                        try:
                                            await websocket.emit(
                                                "depth",
                                                {"type": sub_type, "data": depth_payload},
                                                room=sid,
                                            )
                                        except Exception as e:
                                            logger.error(f"Failed to emit depth to {sid}: {e}")
                                    logger.info(
                                        "[FANOUT][LIVE] event=depth type=%s instrument_id=%s recipients=%s msg_id=%s",
                                        sub_type,
                                        instrument_id,
                                        len(subscribers),
                                        msg_id,
                                    )
                                elif sub_type == "feature.option_chain":
                                    def _to_int(val):
                                        try:
                                            return int(float(val))
                                        except Exception:
                                            return None

                                    option_payload = {
                                        "instrument_id": _to_int(payload_data.get("instrument_id")),
                                        "underlying_instrument_id": _to_int(payload_data.get("underlying_instrument_id")),
                                        "underlying_future_instrument_id": _to_int(payload_data.get("underlying_future_instrument_id")),
                                        "option_type": payload_data.get("option_type"),
                                        "strike": _parse_float(payload_data.get("strike")),
                                        "expiry": payload_data.get("expiry"),
                                        "t_days": _parse_float(payload_data.get("t_days")),
                                        "underlying_price": _parse_float(payload_data.get("underlying_price")),
                                        "option_price": _parse_float(payload_data.get("option_price")),
                                        "implied_vol": _parse_float(payload_data.get("implied_vol")),
                                        "theoretical_price": _parse_float(payload_data.get("theoretical_price")),
                                        "delta": _parse_float(payload_data.get("delta")),
                                        "gamma": _parse_float(payload_data.get("gamma")),
                                        "vega": _parse_float(payload_data.get("vega")),
                                        "theta": _parse_float(payload_data.get("theta")),
                                        "rho": _parse_float(payload_data.get("rho")),
                                        "exchange_ts": payload_data.get("exchange_ts"),
                                        "ingest_ts": payload_data.get("ingest_ts"),
                                    }

                                    for sid in subscribers:
                                        try:
                                            await websocket.emit(
                                                "option_feature",
                                                {"type": sub_type, "data": option_payload},
                                                room=sid,
                                            )
                                        except Exception as e:
                                            logger.error(f"Failed to emit option_feature to {sid}: {e}")
                                    logger.info(
                                        "[FANOUT][LIVE] event=option_feature type=%s instrument_id=%s recipients=%s msg_id=%s",
                                        sub_type,
                                        instrument_id,
                                        len(subscribers),
                                        msg_id,
                                    )

                            await redis_conn.xack(stream_key, FANOUT_CONSUMER_GROUP, msg_id)

                        except Exception as e:
                            logger.error(f"Error processing message from {stream_key}: {e}")
                            await redis_conn.xack(stream_key, FANOUT_CONSUMER_GROUP, msg_id)

            except asyncio.CancelledError:
                logger.info("[FANOUT] Stream loop cancelled")
                break
            except Exception as e:
                logger.exception(f"[FANOUT] Error in stream loop: {e}")
                await asyncio.sleep(1)

        await redis_conn.close()

    async def order_events_loop():
        redis_conn = redis.Redis(connection_pool=async_redis_pool)
        try:
            await redis_conn.xgroup_create(
                name=ORDER_EVENTS_STREAM,
                groupname=ORDER_EVENTS_CONSUMER_GROUP,
                id="$",
                mkstream=True,
            )
            logger.info(f"Created order events consumer group: {ORDER_EVENTS_CONSUMER_GROUP}")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Failed to create order events group: {e}")

        while True:
            try:
                resp = await redis_conn.xreadgroup(
                    groupname=ORDER_EVENTS_CONSUMER_GROUP,
                    consumername=ORDER_EVENTS_CONSUMER_NAME,
                    streams={ORDER_EVENTS_STREAM: ">"},
                    count=100,
                    block=1000,
                )

                if not resp:
                    continue

                for stream_key, messages in resp:
                    for msg_id, values in messages:
                        try:
                            payload = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in values.items()}
                            event_type = payload.get("event_type")

                            if event_type not in ORDER_EVENT_TYPES:
                                await redis_conn.xack(stream_key, ORDER_EVENTS_CONSUMER_GROUP, msg_id)
                                continue

                            details = payload.get("details")
                            parsed_details = None
                            if details:
                                try:
                                    parsed_details = json.loads(details)
                                except Exception:
                                    parsed_details = None

                            # Fetch full bracket data for incremental frontend updates
                            bracket_id = payload.get("bracket_id")
                            bracket_data = None
                            if bracket_id:
                                try:
                                    bracket_hash = await redis_conn.hgetall(f"oms:bracket:{bracket_id}")
                                    if bracket_hash:
                                        bracket_data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v 
                                            for k, v in bracket_hash.items()
                                        }
                                        # Skip emitting events for soft-deleted brackets
                                        if bracket_data.get("deleted") in ["1", 1, "true", True]:
                                            logger.debug(f"Skipping event for deleted bracket {bracket_id}")
                                            await redis_conn.xack(stream_key, ORDER_EVENTS_CONSUMER_GROUP, msg_id)
                                            continue
                                        # Normalize symbol field
                                        if bracket_data.get("symbol") and not bracket_data.get("trading_symbol"):
                                            bracket_data["trading_symbol"] = bracket_data.get("symbol")
                                except Exception as e:
                                    logger.error(f"Failed to fetch bracket {bracket_id}: {e}")

                            event_payload = {
                                "type": "order_event",
                                "event": event_type,
                                "bracket_id": bracket_id,
                                "order_id": payload.get("order_id"),
                                "timestamp": payload.get("timestamp"),
                                "details": parsed_details,
                                "bracket": bracket_data,  # Full bracket data for incremental update
                            }
                            logger.info(f"[FANOUT] Emitting order event: {event_type} for bracket {bracket_id}")
                            
                            # Emit to all authenticated users (connected sockets)
                            # Get all connected rooms/sessions
                            manager = websocket.manager
                            rooms = manager.rooms.get("/", {})
                            
                            emitted_count = 0
                            for sid in rooms.keys():
                                if sid:  # Skip None or empty SIDs
                                    try:
                                        await websocket.emit("order_event", event_payload, room=sid)
                                        emitted_count += 1
                                    except Exception as e:
                                        logger.error(f"Failed to emit order_event to {sid}: {e}")
                            logger.info(
                                "[FANOUT][LIVE] event=order_event type=%s bracket_id=%s recipients=%s msg_id=%s",
                                event_type,
                                bracket_id,
                                emitted_count,
                                msg_id,
                            )
                            
                            await redis_conn.xack(stream_key, ORDER_EVENTS_CONSUMER_GROUP, msg_id)

                        except Exception as e:
                            logger.error(f"Error processing order event {msg_id}: {e}")
                            await redis_conn.xack(stream_key, ORDER_EVENTS_CONSUMER_GROUP, msg_id)

            except asyncio.CancelledError:
                logger.info("[FANOUT] Order events loop cancelled")
                break
            except Exception as e:
                logger.exception(f"[FANOUT] Error in order events loop: {e}")
                await asyncio.sleep(1)

        await redis_conn.close()

    bars_task = asyncio.create_task(stream_loop())
    orders_task = asyncio.create_task(order_events_loop())

    try:
        await asyncio.gather(bars_task, orders_task)
    except asyncio.CancelledError:
        bars_task.cancel()
        orders_task.cancel()
        await asyncio.gather(bars_task, orders_task, return_exceptions=True)
        logger.info("[FANOUT] WebSocket fanout service cancelled")
    finally:
        logger.info("[FANOUT] WebSocket fanout service stopped")


# ==============================================================================
# Websocket handlers


@websocket.on("connect")
async def connect(sid, environ):

    # print(f"----------------------- Websocket CONNECTED: {sid}")
    logger.info(f"Websocket -------- Initiated connection: {sid}")

    # # To get a list of all rooms in the default namespace
    # all_rooms = websocket.manager.rooms["/"].keys()
    # print(f"----------------------- All rooms in default namespace: {list(all_rooms)}")


@websocket.on("disconnect")
async def disconnect(sid, reason):

    await subscription_end(
        sid=sid,
        async_redis_pool=asgi_app.state.async_redis_pool,
    )
    await _stop_all_replay_tasks_for_sid(sid)
    # print(f"----------------------- Websocket DISCONNECTED: {sid} {reason}")
    logger.info(f"Websocket -------- Disconnected connection: {sid} : {reason}")


@websocket.on("authenticate")
async def authenticate(sid, data):
    access_token = data.get("access_token")

    if not access_token:
        await websocket.emit("unauthorized", {"message": "Token Missing"})
        await websocket.disconnect(sid)
        return

    try:
        auth.verify_access_token(access_token)
        await websocket.emit("authenticated", room=sid)
        await subscription_start(
            sid,
            async_redis_pool=asgi_app.state.async_redis_pool,
        )
        print(f"----------------------- Websocket AUTHENTICATED: {sid}")
        logger.info(f"Websocket -------- Authenticated connection: {sid}")

    except Exception as e:
        # print("------------------", e)
        await websocket.emit("unauthorized", {"message": "Token Invalid"}, room=sid)
        await websocket.disconnect(sid)
        print(f"----------------------- Websocket UN-AUTHORIZED: {sid}", e)
        logger.info(f"Websocket -------- Unauthorized connection: {sid}")


@websocket.on("subscribe")
async def subscribe(sid, data):
    """
    Subscribe to instrument_ids for a specific subscription type.
    
    Expected data format:
    {
        "type": "bars.1m",  # or "bars.1D"
        "instruments": [738561, 738817, ...]  # List of instrument_ids
    }
    """
    try:
        sub_type = data.get("type")
        instruments = data.get("instruments", [])
        logger.info(
            "Websocket -------- Subscribe requested: sid=%s type=%s count=%s",
            sid,
            sub_type,
            len(instruments) if isinstance(instruments, list) else "invalid",
        )
        
        # Validate subscription type
        if not sub_type or sub_type not in STREAM_SUBSCRIPTIONS:
            await websocket.emit(
                "error",
                {"message": f"Invalid subscription type. Must be one of: {list(STREAM_SUBSCRIPTIONS.keys())}"},
                room=sid,
            )
            return
        
        if not isinstance(instruments, list):
            await websocket.emit("error", {"message": "'instruments' must be a list"}, room=sid)
            return
        
        # Validate all are integers
        try:
            instruments = [int(inst) for inst in instruments]
        except (ValueError, TypeError) as e:
            await websocket.emit("error", {"message": f"Invalid instrument ID: {e}"}, room=sid)
            return

        await subscription_add(
            sid=sid,
            sub_type=sub_type,
            instruments=instruments,
            async_redis_pool=asgi_app.state.async_redis_pool,
        )
        
        await websocket.emit(
            "subscribed",
            {"type": sub_type, "instruments": instruments, "count": len(instruments)},
            room=sid
        )
        logger.info(f"Websocket -------- User {sid} subscribed to {len(instruments)} instruments for {sub_type}")
        if sub_type == "feature.option_chain":
            logger.info(
                "Websocket -------- Option chain subscription accepted: sid=%s instruments=%s",
                sid,
                instruments[:10],
            )
        
    except Exception as e:
        logger.error(f"Websocket -------- Subscribe error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Subscribe failed: {str(e)}"}, room=sid)


@websocket.on("unsubscribe")
async def unsubscribe(sid, data):
    """
    Unsubscribe from instrument_ids for a specific subscription type.
    
    Expected data format:
    {
        "type": "bars.1m",  # or "bars.1D"
        "instruments": [738561, 738817, ...]  # List of instrument_ids
    }
    """
    try:
        sub_type = data.get("type")
        instruments = data.get("instruments", [])
        logger.info(
            "Websocket -------- Unsubscribe requested: sid=%s type=%s count=%s",
            sid,
            sub_type,
            len(instruments) if isinstance(instruments, list) else "invalid",
        )
        
        # Validate subscription type
        if not sub_type or sub_type not in STREAM_SUBSCRIPTIONS:
            await websocket.emit(
                "error",
                {"message": f"Invalid subscription type. Must be one of: {list(STREAM_SUBSCRIPTIONS.keys())}"},
                room=sid,
            )
            return
        
        if not isinstance(instruments, list):
            await websocket.emit("error", {"message": "'instruments' must be a list"}, room=sid)
            return
        
        # Validate all are integers
        try:
            instruments = [int(inst) for inst in instruments]
        except (ValueError, TypeError) as e:
            await websocket.emit("error", {"message": f"Invalid instrument ID: {e}"}, room=sid)
            return
        
        await subscription_remove(
            sid=sid,
            sub_type=sub_type,
            instruments=instruments,
            async_redis_pool=asgi_app.state.async_redis_pool,
        )
        
        await websocket.emit(
            "unsubscribed",
            {"type": sub_type, "instruments": instruments, "count": len(instruments)},
            room=sid
        )
        logger.info(f"Websocket -------- User {sid} unsubscribed from {len(instruments)} instruments for {sub_type}")
        if sub_type == "feature.option_chain":
            logger.info(
                "Websocket -------- Option chain unsubscribe accepted: sid=%s instruments=%s",
                sid,
                instruments[:10],
            )
        
    except Exception as e:
        logger.error(f"Websocket -------- Unsubscribe error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Unsubscribe failed: {str(e)}"}, room=sid)


@websocket.on("get_subscriptions")
async def get_subscriptions(sid, data):
    """Get list of all instruments the user is currently subscribed to."""
    try:
        sub_type = data.get("type")
        if not sub_type or sub_type not in STREAM_SUBSCRIPTIONS:
            await websocket.emit(
                "error",
                {
                    "message": f"Invalid subscription type. Must be one of: {list(STREAM_SUBSCRIPTIONS.keys())}"
                },
                room=sid,
            )
            return

        async with redis.Redis(connection_pool=asgi_app.state.async_redis_pool) as redis_conn:
            instrument_set = await redis_conn.smembers(f"ws:user.{sid}.{sub_type}.instruments")
        instruments = [int(inst_id) for inst_id in instrument_set]
        
        await websocket.emit(
            "subscriptions",
            {"type": sub_type, "instruments": instruments, "count": len(instruments)},
            room=sid
        )
        
    except Exception as e:
        logger.error(f"Websocket -------- Get subscriptions error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Get subscriptions failed: {str(e)}"}, room=sid)


@websocket.on("subscribe_replay")
async def subscribe_replay(sid, data):
    """
    Subscribe a user to replay bars for one session.

    Expected payload:
    {
      "type": "bars.1m" | "bars.1D",
      "session_id": "<replay_session_id>"
    }
    """
    try:
        sub_type = data.get("type")
        session_id = str(data.get("session_id", "")).strip()

        if sub_type not in REPLAY_BAR_TYPES:
            await websocket.emit(
                "error",
                {
                    "message": f"Invalid replay subscription type. Must be one of: {sorted(REPLAY_BAR_TYPES)}"
                },
                room=sid,
            )
            return

        if not session_id:
            await websocket.emit("error", {"message": "session_id is required"}, room=sid)
            return

        task_key = _replay_sub_task_key(sid, sub_type, session_id)
        if task_key in REPLAY_SUB_TASKS:
            await websocket.emit(
                "subscribed_replay",
                {"type": sub_type, "session_id": session_id, "status": "already_subscribed"},
                room=sid,
            )
            return

        task = asyncio.create_task(
            _replay_stream_reader(
                sid=sid,
                sub_type=sub_type,
                session_id=session_id,
                async_redis_pool=asgi_app.state.async_redis_pool,
            )
        )
        REPLAY_SUB_TASKS[task_key] = task

        await websocket.emit(
            "subscribed_replay",
            {"type": sub_type, "session_id": session_id, "status": "ok"},
            room=sid,
        )
    except Exception as e:
        logger.error(f"Websocket -------- Replay subscribe error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Replay subscribe failed: {str(e)}"}, room=sid)


@websocket.on("unsubscribe_replay")
async def unsubscribe_replay(sid, data):
    """
    Unsubscribe a user from replay bars for one session.

    Expected payload:
    {
      "type": "bars.1m" | "bars.1D",
      "session_id": "<replay_session_id>"
    }
    """
    try:
        sub_type = data.get("type")
        session_id = str(data.get("session_id", "")).strip()

        if sub_type not in REPLAY_BAR_TYPES:
            await websocket.emit(
                "error",
                {
                    "message": f"Invalid replay subscription type. Must be one of: {sorted(REPLAY_BAR_TYPES)}"
                },
                room=sid,
            )
            return

        if not session_id:
            await websocket.emit("error", {"message": "session_id is required"}, room=sid)
            return

        await _stop_replay_task(sid=sid, sub_type=sub_type, session_id=session_id)
        await websocket.emit(
            "unsubscribed_replay",
            {"type": sub_type, "session_id": session_id, "status": "ok"},
            room=sid,
        )
    except Exception as e:
        logger.error(f"Websocket -------- Replay unsubscribe error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Replay unsubscribe failed: {str(e)}"}, room=sid)
