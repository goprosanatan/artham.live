import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from decouple import config
from redis.asyncio import Redis
import psycopg
from psycopg.rows import dict_row


def time_converter(*args):
    return datetime.now(ZoneInfo("Asia/Kolkata")).timetuple()


logger = logging.getLogger(__name__)
logging.Formatter.converter = time_converter
logging.basicConfig(
    filename=(os.path.join(config("DIR_LOGS", cast=str), "artham_replay_01_engine1.log")),
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

REPLAY_CONTROL_STREAM = config("REPLAY_CONTROL_STREAM", cast=str, default="replay:control")
REPLAY_STREAM_PREFIX = config("REPLAY_STREAM_PREFIX", cast=str, default="replay")
REPLAY_TICKS_MAXLEN = config("REPLAY_TICKS_MAXLEN", cast=int, default=200000)
REPLAY_SESSION_KEY_PREFIX = config(
    "REPLAY_SESSION_KEY_PREFIX", cast=str, default="replay:session:"
)

# PostgreSQL connection config
POSTGRES_HOST = config("POSTGRES_HOST", cast=str, default="localhost")
POSTGRES_PORT = config("POSTGRES_PORT", cast=int, default=5432)
POSTGRES_USER = config("POSTGRES_USER", cast=str)
POSTGRES_PASSWORD = config("POSTGRES_PASSWORD", cast=str)
POSTGRES_DB = config("POSTGRES_DB", cast=str)


class ReplaySession:
    def __init__(self, session_id: str, payload: dict):
        self.session_id = session_id
        self.payload = payload
        self.run_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.run_event.set()
        self.task = None


SESSIONS = {}


def replay_stream_key(session_id: str, stream_tail: str) -> str:
    return f"{REPLAY_STREAM_PREFIX}:{session_id}:{stream_tail}"


def normalize_tick_for_redis(tick: dict) -> dict:
    """
    Normalize tick payload to match tick_ingestor format.
    Converts:
    - bool → int
    - datetime → ISO 8601 string
    - Preserves all other types as-is
    
    This ensures replay ticks have identical schema to live ingested ticks.
    """
    normalized = {}
    for key, value in tick.items():
        if isinstance(value, bool):
            # Convert boolean True/False to 1/0 for Redis storage
            normalized[key] = int(value)
        elif isinstance(value, datetime):
            # Convert datetime to ISO 8601 string with timezone
            normalized[key] = value.isoformat()
        else:
            # Keep other types as-is (numeric, str, None, etc.)
            normalized[key] = value
    return normalized


def replay_session_key(session_id: str) -> str:
    return f"{REPLAY_SESSION_KEY_PREFIX}{session_id}"


async def update_session_status(
    redis_conn: Redis, session_id: str, status: str, details: str | None = None
):
    payload = {
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if details:
        payload["details"] = details
    print(f"[DEBUG] update_session_status: session_id={session_id}, status={status}, details={details}")
    await redis_conn.hset(replay_session_key(session_id), mapping=payload)
    # Verify the update was successful
    verify = await redis_conn.hget(replay_session_key(session_id), "status")
    print(f"[DEBUG] update_session_status verified: session_id={session_id}, status_in_redis={verify}")


async def publish_clock(redis_conn: Redis, session_id: str, ts_ms: int):
    clock_state_key = replay_stream_key(session_id, "clock:state")
    clock_stream_key = replay_stream_key(session_id, "clock:stream")

    payload = {
        "session_id": session_id,
        "ts_ms": str(ts_ms),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    await redis_conn.hset(clock_state_key, mapping=payload)
    await redis_conn.xadd(
        clock_stream_key,
        payload,
        maxlen=100000,
        approximate=True,
    )


async def publish_tick(redis_conn: Redis, session_id: str, tick_payload: dict):
    """
    Publish a normalized tick to the replay tick stream.
    Converts all values to strings for Redis storage (matches tick_ingestor format).
    """
    ticks_stream = replay_stream_key(session_id, "md:ticks")
    # Convert all values to strings for Redis (handles numeric, datetime-iso strings, ints, etc.)
    safe_payload = {
        str(k): str(v) if v is not None else ""
        for k, v in tick_payload.items()
    }
    await redis_conn.xadd(
        ticks_stream,
        safe_payload,
        maxlen=REPLAY_TICKS_MAXLEN,
        approximate=True,
    )


async def load_historical_ticks(
    instrument_id: int,
    timestamp_start_ms: int,
    timestamp_end_ms: int,
) -> list:
    """
    Query historical ticks from TimescaleDB for the given time window.
    Returns ALL tick columns to match tick_ingestor streaming format.
    
    Args:
        instrument_id: ID of the instrument
        timestamp_start_ms: Start timestamp in milliseconds (Unix)
        timestamp_end_ms: End timestamp in milliseconds (Unix)
    
    Returns:
        List of tick records with all columns (exchange_ts, depth, ohlc, etc.)
    """
    try:
        async with await psycopg.AsyncConnection.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
            row_factory=dict_row,
        ) as conn:
            # Convert milliseconds to seconds for UNIX timestamp
            ts_start = timestamp_start_ms / 1000.0
            ts_end = timestamp_end_ms / 1000.0
            
            # Query ALL columns from ticks to match tick_ingestor publishing format
            query = """
            SELECT *
            FROM public.ticks
            WHERE instrument_id = %s
              AND extract(epoch from exchange_ts) >= %s
              AND extract(epoch from exchange_ts) <= %s
            ORDER BY exchange_ts ASC
            LIMIT 100000
            """
            
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, (instrument_id, ts_start, ts_end))
                ticks = await cur.fetchall()
            
            logger.info(f"[REPLAY] loaded {len(ticks)} ticks for instrument {instrument_id} from {timestamp_start_ms}ms to {timestamp_end_ms}ms")
            return ticks or []
    except Exception as e:
        logger.exception(f"[REPLAY] error loading ticks: {e}")
        return []


async def handle_session_start(redis_conn: Redis, session_id: str, payload: dict):
    logger.info(f"[REPLAY] session_start received for {session_id}: {payload}")

    instrument_id = payload.get("instrument_id")
    timestamp_start = payload.get("timestamp_start")
    timestamp_end = payload.get("timestamp_end")

    if not instrument_id or not timestamp_start or not timestamp_end:
        logger.warning(f"[REPLAY] missing required params - instrument_id: {instrument_id}, start: {timestamp_start}, end: {timestamp_end}")
        # Publish bootstrap event to ensure downstream knows we tried
        bootstrap_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        await publish_clock(redis_conn, session_id=session_id, ts_ms=bootstrap_ts)
        await update_session_status(redis_conn, session_id, "failed", "missing_params")
        return

    # Load historical ticks from database
    ticks = await load_historical_ticks(
        instrument_id=int(instrument_id),
        timestamp_start_ms=int(timestamp_start),
        timestamp_end_ms=int(timestamp_end),
    )

    if not ticks:
        logger.warning(f"[REPLAY] no ticks found for instrument {instrument_id} in time range")
        # Still publish a bootstrap clock so UI sees session is active
        bootstrap_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        await publish_clock(redis_conn, session_id=session_id, ts_ms=bootstrap_ts)
        await update_session_status(redis_conn, session_id, "failed", "no_ticks")
        return

    # Publish each historical tick to the replay tick stream
    published_count = 0
    start_publish_time = None
    first_tick_ts = None
    
    for idx, tick in enumerate(ticks):
        try:
            ts_seconds = tick["exchange_ts"].timestamp() if hasattr(tick["exchange_ts"], "timestamp") else tick["exchange_ts"]
            ts_ms = int(ts_seconds * 1000)
            
            # Track first tick timestamp for timing calculations
            if idx == 0:
                first_tick_ts = ts_ms
                start_publish_time = datetime.now(timezone.utc).timestamp() * 1000
            
            # Calculate sleep time based on tick timing and speed
            # If speed=1.0, play at real-time (tick deltas)
            # If speed=2.0, play faster (half the delays)
            # If speed=0.5, play slower (double the delays)
            time_since_first = ts_ms - first_tick_ts
            elapsed_real = (datetime.now(timezone.utc).timestamp() * 1000) - start_publish_time
            expected_elapsed = time_since_first / payload.get("speed", 1.0)
            
            sleep_duration = (expected_elapsed - elapsed_real) / 1000.0  # Convert to seconds
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)

            # Publish clock update
            await publish_clock(redis_conn, session_id=session_id, ts_ms=ts_ms)

            # Normalize and publish the FULL tick to match tick_ingestor format
            normalized_tick = normalize_tick_for_redis(tick)
            # Add session_id and source metadata
            normalized_tick["session_id"] = session_id
            normalized_tick["source"] = "replay_historical"
            
            await publish_tick(redis_conn, session_id=session_id, tick_payload=normalized_tick)
            logger.debug(f"[REPLAY] published tick {idx + 1}/{len(ticks)} for session {session_id}: ts_ms={ts_ms}, price={tick.get('last_price')}, volume={tick.get('volume_traded')}")
            published_count += 1
        except Exception as e:
            logger.exception(f"[REPLAY] error publishing tick for session {session_id}: {e}")

    logger.info(f"[REPLAY] published {published_count} ticks for session {session_id}")
    await update_session_status(redis_conn, session_id, "completed")


async def run_replay_session(redis_conn: Redis, session: ReplaySession):
    await handle_session_start(redis_conn, session.session_id, session.payload)
    while not session.stop_event.is_set():
        await session.run_event.wait()
        if session.stop_event.is_set():
            break
        await asyncio.sleep(0.5)


async def start_replay_session(redis_conn: Redis, session_id: str, payload: dict):
    existing = SESSIONS.get(session_id)
    if existing and existing.task and not existing.task.done():
        logger.info(f"[REPLAY] session already running: {session_id}")
        return existing

    session = ReplaySession(session_id=session_id, payload=payload)
    session.task = asyncio.create_task(run_replay_session(redis_conn, session))
    SESSIONS[session_id] = session
    return session


async def pause_replay_session(session_id: str):
    session = SESSIONS.get(session_id)
    if not session:
        return
    session.run_event.clear()


async def resume_replay_session(session_id: str):
    session = SESSIONS.get(session_id)
    if not session:
        return
    session.run_event.set()


async def restart_replay_session(redis_conn: Redis, session_id: str):
    """Restart a replay session from the beginning"""
    logger.info(f"[REPLAY] restarting session {session_id}")
    
    session = SESSIONS.get(session_id)
    if session and session.task:
        # Cancel existing task
        session.stop_event.set()
        session.run_event.set()
        session.task.cancel()
        try:
            await session.task
        except asyncio.CancelledError:
            pass
    
    # Remove from SESSIONS so a fresh session can be created
    SESSIONS.pop(session_id, None)
    
    # Get session details for restarting
    key = f"replay:session:{session_id}"
    raw = await redis_conn.hgetall(key)
    if not raw:
        logger.warning(f"[REPLAY] session {session_id} not found for restart")
        return
    
    # Convert bytes to strings
    session_data = {}
    for k, v in raw.items():
        key_str = k.decode() if isinstance(k, bytes) else k
        val_str = v.decode() if isinstance(v, bytes) else v
        session_data[key_str] = val_str
    
    # Extract payload from session data
    payload = {
        "instrument_id": session_data.get("instrument_id"),
        "speed": float(session_data.get("speed", 1.0)),
        "timestamp_start": int(session_data.get("timestamp_start", 0)),
        "timestamp_end": int(session_data.get("timestamp_end", 0)),
    }
    
    logger.info(f"[REPLAY] restarting session {session_id} with payload: {payload}")
    
    # Start fresh replay - this will call handle_session_start which loads and publishes ticks
    await start_replay_session(redis_conn, session_id=session_id, payload=payload)


async def stop_replay_session(session_id: str):
    session = SESSIONS.get(session_id)
    if not session:
        return
    session.stop_event.set()
    session.run_event.set()
    if session.task:
        session.task.cancel()
        try:
            await session.task
        except asyncio.CancelledError:
            pass
    SESSIONS.pop(session_id, None)


async def delete_replay_session(session_id: str):
    """Clean up in-memory session when deleted from Redis"""
    session = SESSIONS.get(session_id)
    if not session:
        return
    logger.info(f"[REPLAY] deleting session {session_id} from memory")
    session.stop_event.set()
    session.run_event.set()
    if session.task:
        session.task.cancel()
        try:
            await session.task
        except asyncio.CancelledError:
            pass
    SESSIONS.pop(session_id, None)
    logger.info(f"[REPLAY] session {session_id} cleaned up from memory")


async def handle_control_event(redis_conn: Redis, event: str, session_id: str, payload: dict):
    if event == "session_start":
        await start_replay_session(redis_conn, session_id=session_id, payload=payload)
        return

    if event == "session_pause":
        await pause_replay_session(session_id=session_id)
        return

    if event == "session_resume":
        await resume_replay_session(session_id=session_id)
        return

    if event == "session_restart":
        await restart_replay_session(redis_conn, session_id=session_id)
        return

    if event == "session_delete":
        await delete_replay_session(session_id=session_id)
        return

    logger.info(f"[REPLAY] control event {event} for {session_id}: {payload}")


async def run_control_loop():
    logger.info("[REPLAY] engine starting")
    last_id = "$"

    while True:
        try:
            resp = await REDIS_CONN.xread(
                streams={REPLAY_CONTROL_STREAM: last_id},
                count=100,
                block=1000,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, values in messages:
                    event = values.get("event", "")
                    session_id = values.get("session_id", "")
                    raw_payload = values.get("payload") or "{}"
                    try:
                        payload = json.loads(raw_payload)
                    except Exception:
                        payload = {}

                    if session_id:
                        await handle_control_event(
                            REDIS_CONN,
                            event=event,
                            session_id=session_id,
                            payload=payload,
                        )
                    last_id = msg_id

        except asyncio.CancelledError:
            logger.info("[REPLAY] engine cancelled")
            raise
        except Exception as e:
            logger.exception(f"[REPLAY] engine loop error: {e}")
            await asyncio.sleep(1)


async def main():
    await run_control_loop()


if __name__ == "__main__":
    asyncio.run(main())
