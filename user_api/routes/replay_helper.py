import json
from datetime import datetime, timezone
from typing import Optional
from decouple import config


REPLAY_STREAM_PREFIX = config("REPLAY_STREAM_PREFIX", cast=str, default="replay")
REPLAY_CONTROL_STREAM = config("REPLAY_CONTROL_STREAM", cast=str, default="replay:control")
REPLAY_SESSION_KEY_PREFIX = config(
    "REPLAY_SESSION_KEY_PREFIX", cast=str, default="replay:session:"
)
REPLAY_SESSIONS_INDEX_KEY = config(
    "REPLAY_SESSIONS_INDEX_KEY", cast=str, default="replay:sessions"
)
REPLAY_SESSION_TTL_SECONDS = config(
    "REPLAY_SESSION_TTL_SECONDS", cast=int, default=86400
)


def _session_key(session_id: str) -> str:
    return f"{REPLAY_SESSION_KEY_PREFIX}{session_id}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stream_keys(session_id: str) -> dict:
    prefix = f"{REPLAY_STREAM_PREFIX}:{session_id}"
    return {
        "ticks": f"{prefix}:md:ticks",
        "bar_1m": f"{prefix}:bar:1m",
        "bar_1D": f"{prefix}:bar:1D",
        "clock_state": f"{prefix}:clock:state",
        "clock_stream": f"{prefix}:clock:stream",
    }


async def create_replay_session(
    redis_conn,
    session_id: str,
    instrument_id: Optional[int],
    speed: float,
    timestamp_start: Optional[int],
    timestamp_end: Optional[int],
):
    created_at = _now_iso()
    session_payload = {
        "session_id": session_id,
        "status": "running",
        "instrument_id": "" if instrument_id is None else str(instrument_id),
        "speed": str(speed),
        "timestamp_start": "" if timestamp_start is None else str(timestamp_start),
        "timestamp_end": "" if timestamp_end is None else str(timestamp_end),
        "stream_prefix": f"{REPLAY_STREAM_PREFIX}:{session_id}",
        "created_at": created_at,
        "updated_at": created_at,
    }

    key = _session_key(session_id)
    await redis_conn.hset(key, mapping=session_payload)
    await redis_conn.expire(key, REPLAY_SESSION_TTL_SECONDS)
    await redis_conn.sadd(REPLAY_SESSIONS_INDEX_KEY, session_id)

    control_payload = {
        "event": "session_start",
        "session_id": session_id,
        "timestamp": created_at,
        "payload": json.dumps(
            {
                "instrument_id": instrument_id,
                "speed": speed,
                "timestamp_start": timestamp_start,
                "timestamp_end": timestamp_end,
            }
        ),
    }
    await redis_conn.xadd(REPLAY_CONTROL_STREAM, control_payload, maxlen=10000, approximate=True)

    return {
        **session_payload,
        "stream_keys": _stream_keys(session_id),
    }


async def get_replay_session(redis_conn, session_id: str):
    key = _session_key(session_id)
    raw = await redis_conn.hgetall(key)
    if not raw:
        return None
    # Convert bytes to strings if needed
    payload = {}
    for k, v in raw.items():
        key_str = k.decode() if isinstance(k, bytes) else k
        val_str = v.decode() if isinstance(v, bytes) else v
        payload[key_str] = val_str
    payload["stream_keys"] = _stream_keys(session_id)
    return payload


async def list_replay_sessions(redis_conn):
    session_ids = await redis_conn.smembers(REPLAY_SESSIONS_INDEX_KEY)
    if not session_ids:
        return []

    sessions = []
    for sid in sorted(session_ids):
        session_payload = await get_replay_session(redis_conn, sid)
        if session_payload:
            print(f"[DEBUG] list_replay_sessions: session_id={sid}, status={session_payload.get('status')}, updated_at={session_payload.get('updated_at')}")
            sessions.append(session_payload)
    return sessions


async def control_replay_session(redis_conn, session_id: str, action: str):
    key = _session_key(session_id)
    exists = await redis_conn.exists(key)
    if not exists:
        return None

    normalized = action.lower().strip()
    status_map = {
        "pause": "paused",
        "resume": "running",
        "restart": "running",
    }
    if normalized not in status_map:
        raise ValueError("Invalid action. Use pause, resume, or restart")

    updated_at = _now_iso()
    
    # For restart action, we need to reset the session but keep it in the index
    if normalized == "restart":
        print(f"[DEBUG] Clearing streams for restart of session {session_id}")
        
        # Use the proper stream key prefix
        prefix = f"replay:{session_id}"
        stream_keys_to_clear = [
            f"{prefix}:md:ticks",
            f"{prefix}:bar:1m",
            f"{prefix}:bar:1D",
            f"{prefix}:clock:state",
            f"{prefix}:clock:stream",
        ]
        
        # Clear all session streams
        for stream_key in stream_keys_to_clear:
            deleted = await redis_conn.delete(stream_key)
            print(f"[DEBUG] Deleted stream {stream_key}, result: {deleted}")
        
        # Also clear any bar_builder state keys if they exist
        state_keys_to_clear = [
            f"{prefix}:bar_builder:1m:state",
            f"{prefix}:bar_builder:1D:state",
        ]
        for state_key in state_keys_to_clear:
            deleted = await redis_conn.delete(state_key)
            if deleted:
                print(f"[DEBUG] Deleted state key {state_key}, result: {deleted}")
    
    await redis_conn.hset(
        key,
        mapping={
            "status": status_map[normalized],
            "updated_at": updated_at,
        },
    )

    control_payload = {
        "event": f"session_{normalized}",
        "session_id": session_id,
        "timestamp": updated_at,
        "payload": json.dumps({"action": normalized}),
    }
    await redis_conn.xadd(REPLAY_CONTROL_STREAM, control_payload, maxlen=10000, approximate=True)

    return await get_replay_session(redis_conn, session_id)


async def delete_replay_session(redis_conn, session_id: str):
    """
    Delete a replay session and all its associated data from Redis.
    
    This removes:
    - Session metadata hash
    - Per-session tick stream
    - Per-session bar streams (1m, 1D)
    - Per-session clock streams
    - Session from index
    
    Sends a delete control event to notify replay_01_engine to clean up in-memory session.
    
    Args:
        redis_conn: Redis connection
        session_id: ID of the session to delete
    
    Returns:
        True if session was found and deleted, False if not found
    """
    key = _session_key(session_id)
    exists = await redis_conn.exists(key)
    if not exists:
        return False

    stream_keys = _stream_keys(session_id)
    
    # Delete all session-related keys
    keys_to_delete = [
        key,  # Session metadata hash
        stream_keys["ticks"],  # Tick stream
        stream_keys["bar_1m"],  # 1m bar stream
        stream_keys["bar_1D"],  # 1D bar stream
        stream_keys["clock_state"],  # Clock state hash
        stream_keys["clock_stream"],  # Clock stream
    ]
    
    # Delete all keys
    for k in keys_to_delete:
        await redis_conn.delete(k)
    
    # Remove from sessions index
    await redis_conn.srem(REPLAY_SESSIONS_INDEX_KEY, session_id)
    
    # Send delete control event to notify replay_01_engine to clean up in-memory session
    delete_payload = {
        "event": "session_delete",
        "session_id": session_id,
        "timestamp": _now_iso(),
        "payload": json.dumps({}),
    }
    await redis_conn.xadd(REPLAY_CONTROL_STREAM, delete_payload, maxlen=10000, approximate=True)
    
    return True

