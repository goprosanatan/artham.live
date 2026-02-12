import socketio
import redis.asyncio as redis
import logging
import asyncio
import json
from typing import Set, Dict
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
        for sub_type in BAR_STREAMS.keys():
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
    "TARGET_FILLED",
    "STOPLOSS_FILLED",
    "BRACKET_CANCELLED",
    "ORDER_REJECTED",
}



async def init_consumer_groups(redis_conn):
    """Create consumer groups for bar streams if they don't exist."""
    for sub_type, stream_key in BAR_STREAMS.items():
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
    for sub_type in BAR_STREAMS.keys():
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

    async def bars_loop():
        redis_conn = redis.Redis(connection_pool=async_redis_pool)
        await init_consumer_groups(redis_conn)
        streams_dict = {stream_key: ">" for stream_key in BAR_STREAMS.values()}

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
                    sub_type = next((st for st, sk in BAR_STREAMS.items() if sk == stream_key), None)
                    if not sub_type:
                        logger.warning(f"Unknown stream key: {stream_key}")
                        continue

                    for msg_id, values in messages:
                        try:
                            bar_data = {}
                            for key, value in values.items():
                                k = key.decode() if isinstance(key, bytes) else key
                                v = value.decode() if isinstance(value, bytes) else value
                                bar_data[k] = v

                            instrument_id = bar_data.get("instrument_id")
                            if not instrument_id or instrument_id == "None":
                                logger.warning(f"Message {msg_id} has invalid instrument_id: {instrument_id}, skipping")
                                await redis_conn.xack(stream_key, FANOUT_CONSUMER_GROUP, msg_id)
                                continue

                            instrument_id = str(instrument_id)
                            subscribers = await redis_conn.smembers(f"ws:instrument.{sub_type}.{instrument_id}.users")

                            if subscribers:
                                for sid in subscribers:
                                    try:
                                        print(f"Emitting {sub_type} bar for instrument {instrument_id} to user {sid}")
                                        await websocket.emit(
                                            "bar",
                                            {"type": sub_type, "data": bar_data},
                                            room=sid,
                                        )
                                    except Exception as e:
                                        logger.error(f"Failed to emit to {sid}: {e}")

                                logger.debug(
                                    f"[FANOUT] Sent {sub_type} bar for instrument {instrument_id} to {len(subscribers)} users"
                                )

                            await redis_conn.xack(stream_key, FANOUT_CONSUMER_GROUP, msg_id)

                        except Exception as e:
                            logger.error(f"Error processing bar message from {stream_key}: {e}")
                            await redis_conn.xack(stream_key, FANOUT_CONSUMER_GROUP, msg_id)

            except asyncio.CancelledError:
                logger.info("[FANOUT] Bars loop cancelled")
                break
            except Exception as e:
                logger.exception(f"[FANOUT] Error in bars loop: {e}")
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
                            
                            for sid in rooms.keys():
                                if sid:  # Skip None or empty SIDs
                                    try:
                                        await websocket.emit("order_event", event_payload, room=sid)
                                    except Exception as e:
                                        logger.error(f"Failed to emit order_event to {sid}: {e}")
                            
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

    bars_task = asyncio.create_task(bars_loop())
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
        
        # Validate subscription type
        if not sub_type or sub_type not in BAR_STREAMS:
            await websocket.emit("error", {"message": f"Invalid subscription type. Must be one of: {list(BAR_STREAMS.keys())}"}, room=sid)
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
        
        # Validate subscription type
        if not sub_type or sub_type not in BAR_STREAMS:
            await websocket.emit("error", {"message": f"Invalid subscription type. Must be one of: {list(BAR_STREAMS.keys())}"}, room=sid)
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
        
    except Exception as e:
        logger.error(f"Websocket -------- Unsubscribe error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Unsubscribe failed: {str(e)}"}, room=sid)


@websocket.on("get_subscriptions")
async def get_subscriptions(sid, data):
    """Get list of all instruments the user is currently subscribed to."""
    try:
        instrument_set = await get_user_instruments(sid, asgi_app.state.async_redis_pool)
        instruments = [int(inst_id) for inst_id in instrument_set]
        
        await websocket.emit(
            "subscriptions",
            {"instruments": instruments, "count": len(instruments)},
            room=sid
        )
        
    except Exception as e:
        logger.error(f"Websocket -------- Get subscriptions error for {sid}: {e}")
        await websocket.emit("error", {"message": f"Get subscriptions failed: {str(e)}"}, room=sid)
